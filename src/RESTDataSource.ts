import type {
  Fetcher,
  FetcherRequestInit,
  FetcherResponse,
} from '@apollo/utils.fetcher';
import type { KeyValueCache } from '@apollo/utils.keyvaluecache';
import type { Logger } from '@apollo/utils.logger';
import type { WithRequired } from '@apollo/utils.withrequired';
import type { Options as HttpCacheSemanticsOptions } from 'http-cache-semantics';
import cloneDeep from 'lodash.clonedeep';
import isPlainObject from 'lodash.isplainobject';
import {
  CACHE_ENTRY_STRING_MARSHALLER,
  HTTPCache,
  ResponseParser,
} from './HTTPCache';
import { GraphQLError } from 'graphql';

export type ValueOrPromise<T> = T | Promise<T>;

export type RequestOptions<CO extends CacheOptions = CacheOptions> =
  WithRequired<FetcherRequestInit, 'method' | 'headers'> & {
    /**
     * URL search parameters can be provided either as a record object (in which
     * case keys with `undefined` values are ignored) or as an URLSearchParams
     * object. If you want to specify a parameter multiple times, use
     * URLSearchParams with its "array of two-element arrays" constructor form.
     * (The URLSearchParams object is globally available in Node, and provided to
     * TypeScript by @types/node.)
     */
    params: URLSearchParams;
    /**
     * The default implementation of `cacheKeyFor` returns this value if it is
     * provided. This is used both as part of the request deduplication key and as
     * the key in the shared HTTP-header-sensitive cache.
     */
    cacheKey?: string;
    /**
     * This can be a `CacheOptions` object or a (possibly async) function
     * returning such an object. The details of what its fields mean are
     * documented under `CacheOptions`. The function is called after a real HTTP
     * request is made (and is not called if a response from the cache can be
     * returned). If this is provided, the `cacheOptionsFor` hook is not called.
     */
    cacheOptions?:
      | CO
      | ((
          url: string,
          response: FetcherResponse,
          request: RequestOptions<CO>,
        ) => ValueOrPromise<CO | undefined>);
    /**
     * If provided, this is passed through as the third argument to `new
     * CachePolicy()` from the `http-cache-semantics` npm package as part of the
     * HTTP-header-sensitive cache.
     */
    httpCacheSemanticsCachePolicyOptions?: HttpCacheSemanticsOptions;
  };

export type DataSourceRequest<CO extends CacheOptions = CacheOptions> = Omit<
  RequestOptions<CO>,
  'method' | 'headers' | 'params' | 'body'
> & {
  method?: RequestOptions['method']; // Allow optional method
  headers?: RequestOptions['headers']; // Allow optional headers
  params?: RequestOptions['params'] | Record<string, string | undefined>; // Allow record as query params
  body?: RequestOptions['body'] | object; // Allow object as body
};

export interface HeadRequest<CO extends CacheOptions = CacheOptions>
  extends DataSourceRequest<CO> {
  method?: 'HEAD';
  body?: never;
}

export interface GetRequest<CO extends CacheOptions = CacheOptions>
  extends DataSourceRequest<CO> {
  method?: 'GET';
  body?: never;
}

export interface PostRequest<CO extends CacheOptions = CacheOptions>
  extends DataSourceRequest<CO> {
  method?: 'POST';
}

export interface PutRequest<CO extends CacheOptions = CacheOptions>
  extends DataSourceRequest<CO> {
  method?: 'PUT';
}

export interface PatchRequest<CO extends CacheOptions = CacheOptions>
  extends DataSourceRequest<CO> {
  method?: 'PATCH';
}

export interface DeleteRequest<CO extends CacheOptions = CacheOptions>
  extends DataSourceRequest<CO> {
  method?: 'DELETE';
}

export type RequestWithoutBody<CO extends CacheOptions = CacheOptions> =
  | HeadRequest<CO>
  | GetRequest<CO>;

export type RequestWithBody<CO extends CacheOptions = CacheOptions> =
  | PostRequest<CO>
  | PutRequest<CO>
  | PatchRequest<CO>
  | DeleteRequest<CO>;

export interface CacheOptions {
  /**
   * This sets the TTL used in the shared cache to a value in seconds. If this
   * is 0, the response will not be stored. If this is a positive number  and
   * the operation returns a 2xx status code, then the response *will* be
   * cached, regardless of HTTP headers or method: make sure this is what you
   * intended! (There is currently no way to say "only cache responses that
   * should be cached according to HTTP headers, but change the TTL to something
   * specific".) Note that if this is not provided, only `GET` requests are
   * cached.
   */
  ttl?: number;
}

const NODE_ENV = process.env.NODE_ENV;

export interface DataSourceConfig {
  cache?: KeyValueCache;
  fetch?: Fetcher;
  logger?: Logger;
}

export interface RequestDeduplicationResult {
  policy: RequestDeduplicationPolicy;
  deduplicatedAgainstPreviousRequest: boolean;
}

export interface HTTPCacheResult {
  // This is primarily returned so that tests can be deterministic.
  cacheWritePromise: Promise<void> | undefined;
}

export interface DataSourceResult<TResult> {
  result: TResult;
  requestDeduplication: RequestDeduplicationResult;
  httpCache: HTTPCacheResult;
}

// RESTDataSource has two layers of caching. The first layer is purely in-memory
// within a single RESTDataSource object and is called "request deduplication".
// It is primarily designed so that multiple identical GET requests started
// concurrently can share one real HTTP GET; it does not observe HTTP response
// headers. (The second layer uses a potentially shared KeyValueCache for
// storage and does observe HTTP response headers.) To configure request
// deduplication, override requestDeduplicationPolicyFor.
export type RequestDeduplicationPolicy =
  // If a request with the same deduplication key is in progress, share its
  // result. Otherwise, start a request, allow other requests to de-duplicate
  // against it while it is running, and forget about it once the request returns
  // successfully.
  | { policy: 'deduplicate-during-request-lifetime'; deduplicationKey: string }
  // If a request with the same deduplication key is in progress, share its
  // result. Otherwise, start a request and allow other requests to de-duplicate
  // against it while it is running. All future requests with policy
  // `deduplicate-during-request-lifetime` or `deduplicate-until-invalidated`
  // with the same `deduplicationKey` will share the same result until a request
  // is started with policy `do-not-deduplicate` and a matching entry in
  // `invalidateDeduplicationKeys`.
  | { policy: 'deduplicate-until-invalidated'; deduplicationKey: string }
  // Always run an actual HTTP request and don't allow other requests to
  // de-duplicate against it. Additionally, invalidate any listed keys
  // immediately: new requests with that deduplicationKey will not match any
  // requests that current exist. (The invalidation feature is used so that
  // doing (say) `DELETE /path` invalidates any result for `GET /path` within
  // the deduplication store.)
  | { policy: 'do-not-deduplicate'; invalidateDeduplicationKeys?: string[] };

export abstract class RESTDataSource<CO extends CacheOptions = CacheOptions> {
  protected httpCache: HTTPCache<CO>;
  protected deduplicationPromises = new Map<string, Promise<any>>();
  baseURL?: string;
  logger: Logger;

  constructor(config?: DataSourceConfig) {
    this.httpCache = new HTTPCache<CO>(
      config?.cache,
      config?.fetch,
      CACHE_ENTRY_STRING_MARSHALLER,
    );
    this.logger = config?.logger ?? console;
  }

  protected async head<TResult = any>(
    path: string,
    request: HeadRequest<CO> = {},
    responseParser?: ResponseParser<TResult>,
  ): Promise<TResult> {
    return (
      await this.fetch<TResult>(
        path,
        {
          method: 'HEAD',
          ...request,
        },
        responseParser,
      )
    ).result;
  }

  protected async get<TResult = any>(
    path: string,
    request: GetRequest<CO> = {},
    responseParser?: ResponseParser<TResult>,
  ): Promise<TResult> {
    return (
      await this.fetch<TResult>(
        path,
        {
          method: 'GET',
          ...request,
        },
        responseParser,
      )
    ).result;
  }

  protected async post<TResult = any>(
    path: string,
    request: PostRequest<CO> = {},
    responseParser?: ResponseParser<TResult>,
  ): Promise<TResult> {
    return (
      await this.fetch<TResult>(
        path,
        {
          method: 'POST',
          ...request,
        },
        responseParser,
      )
    ).result;
  }

  protected async patch<TResult = any>(
    path: string,
    request: PatchRequest<CO> = {},
    responseParser?: ResponseParser<TResult>,
  ): Promise<TResult> {
    return (
      await this.fetch<TResult>(
        path,
        {
          method: 'PATCH',
          ...request,
        },
        responseParser,
      )
    ).result;
  }

  protected async put<TResult = any>(
    path: string,
    request: PutRequest<CO> = {},
    responseParser?: ResponseParser<TResult>,
  ): Promise<TResult> {
    return (
      await this.fetch<TResult>(
        path,
        {
          method: 'PUT',
          ...request,
        },
        responseParser,
      )
    ).result;
  }

  protected async delete<TResult = any>(
    path: string,
    request: DeleteRequest<CO> = {},
    responseParser?: ResponseParser<TResult>,
  ): Promise<TResult> {
    return (
      await this.fetch<TResult>(
        path,
        {
          method: 'DELETE',
          ...request,
        },
        responseParser,
      )
    ).result;
  }

  public async fetch<TResult>(
    path: string,
    dataSourceRequest: DataSourceRequest<CO> = {},
    responseParser?: ResponseParser<TResult>,
  ): Promise<DataSourceResult<TResult>> {
    // Use the default response parser if none was provided.
    if (!responseParser) {
      responseParser = this.responseParser();
    }

    const requestOptions = await this.toRequestOptions(dataSourceRequest);

    // A hook to manipulate the final fetch request and/or the url.
    const url = await this.willSendRequest(path, requestOptions);

    // Cache GET requests based on the calculated cache key
    // Disabling the request cache does not disable the response cache
    const policy = this.requestDeduplicationPolicyFor(url, requestOptions);
    if (
      policy.policy === 'deduplicate-during-request-lifetime' ||
      policy.policy === 'deduplicate-until-invalidated'
    ) {
      const previousRequestPromise = this.deduplicationPromises.get(
        policy.deduplicationKey,
      );
      if (previousRequestPromise)
        return previousRequestPromise.then((result) =>
          this.cloneDataSourceFetchResult(result, {
            policy,
            deduplicatedAgainstPreviousRequest: true,
          }),
        );

      const thisRequestPromise = this.performRequest(
        url,
        requestOptions,
        responseParser,
      );
      this.deduplicationPromises.set(
        policy.deduplicationKey,
        thisRequestPromise,
      );
      try {
        // The request promise needs to be awaited here rather than just
        // returned. This ensures that the request completes before it's removed
        // from the cache. Additionally, the use of finally here guarantees the
        // deduplication cache is cleared in the event of an error during the
        // request.
        //
        // Note: we could try to get fancy and only clone if no de-duplication
        // happened (and we're "deduplicate-during-request-lifetime") but we
        // haven't quite bothered yet.
        return this.cloneDataSourceFetchResult<TResult>(
          await thisRequestPromise,
          {
            policy,
            deduplicatedAgainstPreviousRequest: false,
          },
        );
      } finally {
        if (policy.policy === 'deduplicate-during-request-lifetime') {
          this.deduplicationPromises.delete(policy.deduplicationKey);
        }
      }
    } else {
      for (const key of policy.invalidateDeduplicationKeys ?? []) {
        this.deduplicationPromises.delete(key);
      }
      return {
        ...(await this.performRequest(url, requestOptions, responseParser)),
        requestDeduplication: {
          policy,
          deduplicatedAgainstPreviousRequest: false,
        },
      };
    }
  }

  private async toRequestOptions(
    dataSourceRequest: DataSourceRequest<CO> = {},
  ): Promise<RequestOptions<CO>> {
    const downcasedHeaders: Record<string, string> = {};
    if (dataSourceRequest.headers) {
      // map incoming headers to lower-case headers
      Object.entries(dataSourceRequest.headers).forEach(([key, value]) => {
        downcasedHeaders[key.toLowerCase()] = value;
      });
    }

    let body;
    // FIXME this feels weird and very limiting. What about all other possible serialisations..
    if (this.shouldJSONSerializeBody(dataSourceRequest.body)) {
      body = JSON.stringify(dataSourceRequest.body);
      // If Content-Type header has not been previously set, set to application/json
      if (!downcasedHeaders['content-type']) {
        downcasedHeaders['content-type'] = 'application/json';
      }
    } else {
      body = dataSourceRequest.body;
    }

    return {
      ...dataSourceRequest,
      // Default to GET in the case that `fetch` is called directly with no method
      // provided. Our other request methods all provide one.
      method: dataSourceRequest.method ? dataSourceRequest.method : 'GET',
      headers: downcasedHeaders,
      // guarantee params and headers objects before calling `willSendRequest` for convenience
      params:
        dataSourceRequest.params instanceof URLSearchParams
          ? dataSourceRequest.params
          : this.urlSearchParamsFromRecord(dataSourceRequest.params),
      body,
    };
  }

  protected shouldJSONSerializeBody(
    body: RequestWithBody<CO>['body'],
  ): body is object {
    return !!(
      // We accept arbitrary objects and arrays as body and serialize them as JSON.
      (
        Array.isArray(body) ||
        isPlainObject(body) ||
        // We serialize any objects that have a toJSON method (except Buffers or things that look like FormData)
        (body &&
          typeof body === 'object' &&
          'toJSON' in body &&
          typeof (body as any).toJSON === 'function' &&
          !(body instanceof Buffer) &&
          // XXX this is a bit of a hacky check for FormData-like objects (in
          // case a FormData implementation has a toJSON method on it)
          (body as any).constructor?.name !== 'FormData')
      )
    );
  }

  private urlSearchParamsFromRecord(
    params: Record<string, string | undefined> | undefined,
  ): URLSearchParams {
    const usp = new URLSearchParams();
    if (params) {
      for (const [name, value] of Object.entries(params)) {
        if (value !== undefined && value !== null) {
          usp.set(name, value);
        }
      }
    }
    return usp;
  }

  protected willSendRequest(
    path: string,
    requestOptions: RequestOptions<CO>,
  ): ValueOrPromise<URL> {
    const url = new URL(path, this.baseURL);
    // Append params to existing params in the path
    for (const [name, value] of requestOptions.params) {
      url.searchParams.append(name, value);
    }
    return url;
  }

  private async performRequest<TResult>(
    url: URL,
    requestOptions: RequestOptions<CO>,
    responseParser: ResponseParser<TResult>,
  ): Promise<Omit<DataSourceResult<TResult>, 'requestDeduplication'>> {
    return this.trace(url, requestOptions, async () => {
      const cacheKey = this.cacheKeyFor(url, requestOptions);
      const cacheOptions = requestOptions.cacheOptions
        ? requestOptions.cacheOptions
        : this.cacheOptionsFor?.bind(this);
      const { result, cacheWritePromise } = await this.httpCache.fetch(
        url,
        requestOptions,
        {
          cacheKey,
          cacheOptions,
          httpCacheSemanticsCachePolicyOptions:
            requestOptions.httpCacheSemanticsCachePolicyOptions,
        },
        responseParser,
      );

      if (cacheWritePromise) {
        this.catchCacheWritePromiseErrors(cacheWritePromise);
      }

      return {
        result,
        httpCache: {
          cacheWritePromise,
        },
      };
    });
  }

  /**
   * Calculates the deduplication policy for the request.
   *
   * By default, GET requests have the policy
   * `deduplicate-during-request-lifetime` with deduplication key `GET
   * ${cacheKey}`, and all other requests have the policy `do-not-deduplicate`
   * and invalidate `GET ${cacheKey}`, where `cacheKey` is the value returned by
   * `cacheKeyFor` (and is the same cache key used in the HTTP-header-sensitive
   * shared cache).
   *
   * Note that the default cache key only contains the URL (not the method,
   * headers, body, etc), so if you send multiple GET requests that differ only
   * in headers (etc), or if you change your policy to allow non-GET requests to
   * be deduplicated, you may want to put more information into the cache key or
   * be careful to keep the HTTP method in the deduplication key.
   */
  protected requestDeduplicationPolicyFor(
    url: URL,
    request: RequestOptions<CO>,
  ): RequestDeduplicationPolicy {
    const method = request.method ?? 'GET';
    // Start with the cache key that is used for the shared header-sensitive
    // cache. Note that its default implementation does not include the HTTP
    // method, so if a subclass overrides this and allows non-GET/HEADs to be
    // de-duplicated it will be important for it to include (at least!) the
    // method in the deduplication key, so we're explicitly adding GET/HEAD here.
    const cacheKey = this.cacheKeyFor(url, request);
    if (['GET', 'HEAD'].includes(method)) {
      return {
        policy: 'deduplicate-during-request-lifetime',
        deduplicationKey: cacheKey,
      };
    } else {
      return {
        policy: 'do-not-deduplicate',
        // Always invalidate GETs and HEADs when a different method is seen on
        // the same cache key (ie, URL), as per standard HTTP semantics. (We
        // don't have to invalidate the key with this HTTP method because we
        // never write it.)
        invalidateDeduplicationKeys: [
          this.cacheKeyFor(url, { ...request, method: 'GET' }),
          this.cacheKeyFor(url, { ...request, method: 'HEAD' }),
        ],
      };
    }
  }

  // By default, we use `cacheKey` from the request if provided, or the full
  // request URL. You can override this to remove query parameters or compute a
  // cache key in any way that makes sense. For example, you could use this to
  // take header fields into account (the kinds of fields you expect to show up
  // in Vary in the response). Although we do parse Vary in responses so that we
  // won't return a cache entry whose Vary-ed header field doesn't match, new
  // responses can overwrite old ones with different Vary-ed header fields if
  // you don't take the header into account in the cache key.
  protected cacheKeyFor(url: URL, request: RequestOptions<CO>): string {
    return request.cacheKey ?? `${request.method} ${url}`;
  }

  protected cacheOptionsFor?(
    url: string,
    response: FetcherResponse,
    request: FetcherRequestInit,
  ): ValueOrPromise<CO | undefined>;

  protected responseParser<TResult>(
    okResponseParser?: ResponseParser<TResult>,
    nokResponseParser?: ResponseParser<TResult>,
  ): ResponseParser<TResult> {
    return async (response: FetcherResponse): Promise<TResult> => {
      if (response.ok) {
        if (okResponseParser) {
          return okResponseParser(response);
        } else {
          return (await this.parseBody(response)) as TResult;
        }
      } else {
        if (nokResponseParser) {
          return nokResponseParser(response);
        } else {
          throw await this.errorFromResponse(response);
        }
      }
    };
  }

  // Reads the body of the response and returns it in parsed form. If you want
  // to process data in some other way (eg, reading binary data), override this
  // method. It's important that the body always read in full (otherwise the
  // clone of this response that is being read to write to the HTTPCache could
  // block and lead to a memory leak).
  //
  // If you override this to return interesting new mutable data types, override
  // cloneParsedBody too.
  protected parseBody(response: FetcherResponse): Promise<object | string> {
    const contentType = response.headers.get('Content-Type');
    const contentLength = response.headers.get('Content-Length');
    if (
      // As one might expect, a "204 No Content" is empty! This means there
      // isn't enough to `JSON.parse`, and trying will result in an error.
      response.status !== 204 &&
      contentLength !== '0' &&
      contentType &&
      (contentType.startsWith('application/json') ||
        contentType.endsWith('+json'))
    ) {
      return response.json();
    } else {
      return response.text();
    }
  }

  protected async errorFromResponse(response: FetcherResponse) {
    const codeByStatus = new Map<number, string>([
      [401, 'UNAUTHENTICATED'],
      [403, 'FORBIDDEN'],
    ]);
    const code = codeByStatus.get(response.status);

    return new GraphQLError(`${response.status}: ${response.statusText}`, {
      extensions: {
        ...(code && { code }),
        response: {
          url: response.url,
          status: response.status,
          statusText: response.statusText,
          body: await this.parseBody(response),
        },
      },
    });
  }

  private cloneDataSourceFetchResult<TResult>(
    dataSourceFetchResult: Omit<
      DataSourceResult<TResult>,
      'requestDeduplication'
    >,
    requestDeduplicationResult: RequestDeduplicationResult,
  ): DataSourceResult<TResult> {
    return {
      ...dataSourceFetchResult,
      requestDeduplication: requestDeduplicationResult,
      result: this.cloneParsedBody(dataSourceFetchResult.result),
    };
  }

  protected cloneParsedBody<TResult>(parsedBody: TResult) {
    // consider using `structuredClone()` when we drop support for Node 16
    return cloneDeep(parsedBody);
  }

  // Override this method to handle these errors in a different way.
  protected catchCacheWritePromiseErrors(cacheWritePromise: Promise<void>) {
    cacheWritePromise.catch((e) => {
      this.logger.error(`Error writing from RESTDataSource to cache: ${e}`);
    });
  }

  protected async trace<TResult>(
    url: URL,
    request: RequestOptions<CO>,
    fn: () => Promise<TResult>,
  ): Promise<TResult> {
    if (NODE_ENV === 'development') {
      // We're not using console.time because that isn't supported on Cloudflare
      const startTime = Date.now();
      try {
        return await fn();
      } finally {
        const duration = Date.now() - startTime;
        const label = `${request.method || 'GET'} ${url}`;
        this.logger.info(`${label} (${duration}ms)`);
      }
    } else {
      return fn();
    }
  }
}
