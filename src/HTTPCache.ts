import nodeFetch, { Headers as NodeFetchHeaders } from 'node-fetch';
import type { Options as HttpCacheSemanticsOptions } from 'http-cache-semantics';
import CachePolicy from 'http-cache-semantics';
import type { Fetcher, FetcherResponse } from '@apollo/utils.fetcher';
import {
  InMemoryLRUCache,
  type KeyValueCache,
  PrefixingKeyValueCache,
} from '@apollo/utils.keyvaluecache';
import type {
  CacheOptions,
  RequestOptions,
  ValueOrPromise,
} from './RESTDataSource';
import { GraphQLError } from 'graphql';

// We want to use a couple internal properties of CachePolicy. (We could get
// `_url` and `_status` off of the serialized CachePolicyObject, but `age()` is
// just missing from `@types/http-cache-semantics` for now.) So we just cast to
// this interface for now.
interface SneakyCachePolicy extends CachePolicy {
  _url: string | undefined;
  _status: number;
  age(): number;
}

export interface FetchResult<TResult, TError> {
  result?: TResult;
  error?: TError;
  cacheWritePromise?: Promise<void>;
}

export type ResponseParser<TResult, TError> = (
  response: FetcherResponse,
) => Promise<{
  result?: TResult;
  error?: TError;
}>;

export type CacheEntry = {
  policy: SneakyCachePolicy;
  ttlOverride?: number;
  parsedResponse: {
    result?: any;
    error?: any;
  };
};

export type CacheEntryMarshaller<CacheValue extends {} = string> = {
  deserialize(serializedValue: CacheValue): CacheEntry;
  serialize(cacheEntry: CacheEntry): CacheValue;
};

export const CACHE_ENTRY_STRING_MARSHALLER: CacheEntryMarshaller<string> = {
  deserialize(serializedValue: string): CacheEntry {
    const cacheEntryJson = JSON.parse(serializedValue);
    return {
      ...cacheEntryJson,
      policy: CachePolicy.fromObject(cacheEntryJson.policy),
      parsedResponse: {
        result: cacheEntryJson.parsedResponse.result,
        error:
          cacheEntryJson.parsedResponse.error !== undefined
            ? new GraphQLError(cacheEntryJson.parsedResponse.error)
            : undefined,
      },
    };
  },
  serialize(cacheEntry: CacheEntry): string {
    return JSON.stringify({
      ...cacheEntry,
      policy: cacheEntry.policy.toObject(),
      parsedResponse: {
        result: cacheEntry.parsedResponse.result,
        error:
          cacheEntry.parsedResponse.error instanceof GraphQLError
            ? cacheEntry.parsedResponse.error.toJSON()
            : undefined,
      },
    });
  },
};

export const CACHE_ENTRY_NOOP_MARSHALLER: CacheEntryMarshaller<CacheEntry> = {
  deserialize(cacheEntry: CacheEntry): CacheEntry {
    return cacheEntry;
  },
  serialize(cacheEntry: CacheEntry): CacheEntry {
    return cacheEntry;
  },
};

export class HTTPCache<
  CO extends CacheOptions = CacheOptions,
  CacheValue extends {} = string,
> {
  private keyValueCache: KeyValueCache<CacheValue, CO>;
  private httpFetch: Fetcher;
  private cacheEntryMarshaller: CacheEntryMarshaller<CacheValue>;

  constructor(
    keyValueCache: KeyValueCache<CacheValue> = new InMemoryLRUCache<
      CacheValue,
      CO
    >(),
    httpFetch: Fetcher = nodeFetch,
    cacheEntryMarshaller: CacheEntryMarshaller<CacheValue>,
  ) {
    this.keyValueCache = new PrefixingKeyValueCache(
      keyValueCache,
      'httpcache:',
    );
    this.httpFetch = httpFetch;
    this.cacheEntryMarshaller = cacheEntryMarshaller;
  }

  async fetch<TResult = any, TError = any>(
    url: URL,
    requestOpts: RequestOptions<CO>,
    cache: {
      cacheKey: string;
      cacheOptions?:
        | CO
        | ((
            url: string,
            response: FetcherResponse,
            request: RequestOptions<CO>,
          ) => ValueOrPromise<CO | undefined>);
      httpCacheSemanticsCachePolicyOptions?: HttpCacheSemanticsOptions;
    },
    responseParser: ResponseParser<TResult, TError>,
  ): Promise<FetchResult<TResult, TError>> {
    const urlString = url.toString();
    const cacheKey = cache.cacheKey;

    // Bypass the cache altogether for HEAD requests. Caching them might be fine
    // to do, but for now this is just a pragmatic choice for timeliness without
    // fully understanding the interplay between GET and HEAD requests (i.e.
    // refreshing headers with HEAD requests, responding to HEADs with cached
    // and valid GETs, etc.)
    if (requestOpts.method === 'HEAD') {
      const response = await this.httpFetch(urlString, requestOpts);
      const parsedResponse = await responseParser(response);
      return {
        ...parsedResponse,
      };
    }

    const entry = await this.keyValueCache.get(cacheKey);
    if (!entry) {
      // There's nothing in our cache. Fetch the URL and save it to the cache if
      // we're allowed.
      const response = await this.httpFetch(urlString, requestOpts);

      const parsedResponse = await responseParser(response);

      const policy = new CachePolicy(
        policyRequestFrom(urlString, requestOpts),
        policyResponseFrom(response),
        cache?.httpCacheSemanticsCachePolicyOptions,
      ) as SneakyCachePolicy;

      const cacheOptions =
        typeof cache?.cacheOptions === 'function'
          ? await cache?.cacheOptions(urlString, response, requestOpts)
          : cache?.cacheOptions;

      const ttlFactor = canBeRevalidated(response) ? 2 : undefined;

      return this.storeResponseAndReturnClone(
        cacheKey,
        parsedResponse,
        requestOpts,
        policy,
        cacheOptions,
        ttlFactor,
      );
    }

    const { policy, ttlOverride, parsedResponse } =
      this.cacheEntryMarshaller.deserialize(entry);

    // Remove url from the policy, because otherwise it would never match a
    // request with a custom cache key (ie, we want users to be able to tell us
    // that two requests should be treated as the same even if the URL differs).
    policy._url = undefined;

    //
    const policySatisfiedWhenFresh =
      typeof cache?.cacheOptions === 'function'
        ? false
        : cache?.cacheOptions?.policySatisfiedWhenFresh;

    if (
      (ttlOverride && policy.age() < ttlOverride) ||
      (!ttlOverride &&
        policy.age() <= policy.timeToLive() &&
        !!policySatisfiedWhenFresh) ||
      (!ttlOverride &&
        policy.satisfiesWithoutRevalidation(
          policyRequestFrom(urlString, requestOpts),
        ))
    ) {
      // Either the cache entry was created with an explicit TTL override (ie,
      // `ttl` returned from `cacheOptionsFor`) and we're within that TTL, or
      // the cache entry was not created with an explicit TTL override and the
      // header-based cache policy says we can safely use the cached response.
      return {
        result: parsedResponse.result,
        error: parsedResponse.error,
      };
    } else {
      // We aren't sure that we're allowed to use the cached response, so we are
      // going to actually do a fetch. However, we may have one extra trick up
      // our sleeve. If the cached response contained an `etag` or
      // `last-modified` header, then we can add an appropriate `if-none-match`
      // or `if-modified-since` header to the request. If what we're fetching
      // hasn't changed, then the server can return a small 304 response instead
      // of a large 200, and we can use the body from our cache. This logic is
      // implemented inside `policy.revalidationHeaders`; we support it by
      // setting a larger KeyValueCache TTL for responses with these headers
      // (see `canBeRevalidated`).  (If the cached response doesn't have those
      // headers, we'll just end up fetching the normal request here.)
      //
      // Note that even if we end up able to reuse the cached body here, we
      // still re-write to the cache, because we might need to update the TTL or
      // other aspects of the cache policy based on the headers we got back.
      const revalidationHeaders = policy.revalidationHeaders(
        policyRequestFrom(urlString, requestOpts),
      );
      const revalidationRequest = {
        ...requestOpts,
        headers: cachePolicyHeadersToFetcherHeadersInit(revalidationHeaders),
      };
      const revalidationResponse = await this.httpFetch(
        urlString,
        revalidationRequest,
      );

      const { policy: revalidatedPolicy, modified } = policy.revalidatedPolicy(
        policyRequestFrom(urlString, revalidationRequest),
        policyResponseFrom(revalidationResponse),
      ) as unknown as { policy: SneakyCachePolicy; modified: boolean };

      const cacheOptions =
        typeof cache?.cacheOptions === 'function'
          ? await cache?.cacheOptions(
              urlString,
              revalidationResponse,
              revalidationRequest,
            )
          : cache?.cacheOptions;

      const ttlFactor = canBeRevalidated(revalidationResponse) ? 2 : undefined;

      return this.storeResponseAndReturnClone(
        cacheKey,
        modified ? await responseParser(revalidationResponse) : parsedResponse,
        requestOpts,
        revalidatedPolicy,
        cacheOptions,
        ttlFactor,
      );
    }
  }

  private async storeResponseAndReturnClone<TResult, TError>(
    cacheKey: string,
    parsedResponse: Awaited<ReturnType<ResponseParser<TResult, TError>>>,
    request: RequestOptions<CO>,
    policy: SneakyCachePolicy,
    cacheOptions?: CO,
    ttlFactor?: number,
  ): Promise<FetchResult<TResult, TError>> {
    let ttlOverride = cacheOptions?.ttl;

    if (
      // With a TTL override, only cache successful responses but otherwise ignore method and response headers
      !(ttlOverride && policy._status >= 200 && policy._status <= 299) &&
      // Without an override, we only cache GET requests and respect standard HTTP cache semantics
      !(request.method === 'GET' && policy.storable())
    ) {
      return { ...parsedResponse };
    }

    let ttl =
      ttlOverride === undefined
        ? Math.round(policy.timeToLive() / 1000)
        : ttlOverride;
    if (ttl <= 0) return { ...parsedResponse };

    // If a response can be revalidated, we don't want to remove it from the
    // cache right after it expires. (See the comment above the call to
    // `revalidationHeaders` for details.) We may be able to use better
    // heuristics here, but for now we'll take the max-age times 2.
    if (ttlFactor) {
      ttl *= ttlFactor;
    }

    const cacheEntry: CacheEntry = {
      policy: policy,
      ttlOverride,
      parsedResponse,
    };

    return {
      ...parsedResponse,
      cacheWritePromise: this.writeToCache({
        cacheKey,
        cacheEntry,
        cacheOptions: {
          ...cacheOptions,
          ttl,
        } as CO,
      }),
    };
  }

  private async writeToCache({
    cacheKey,
    cacheEntry,
    cacheOptions,
  }: {
    cacheKey: string;
    cacheEntry: CacheEntry;
    cacheOptions?: CO;
  }): Promise<void> {
    const serialisedCacheEntry =
      this.cacheEntryMarshaller.serialize(cacheEntry);
    // Set the value into the cache, and forward all the set cache option into the setter function
    await this.keyValueCache.set(cacheKey, serialisedCacheEntry, cacheOptions);
  }
}

function canBeRevalidated(response: FetcherResponse): boolean {
  return response.headers.has('ETag') || response.headers.has('Last-Modified');
}

function policyRequestFrom<CO extends CacheOptions = CacheOptions>(
  url: string,
  request: RequestOptions<CO>,
) {
  return {
    url,
    method: request.method ?? 'GET',
    headers: request.headers ?? {},
  };
}

function policyResponseFrom(response: FetcherResponse) {
  return {
    status: response.status,
    headers:
      response.headers instanceof NodeFetchHeaders &&
      // https://github.com/apollo-server-integrations/apollo-server-integration-cloudflare-workers/issues/37
      // For some reason, Cloudflare Workers' `response.headers` is passing
      // the instanceof check here but doesn't have the `raw()` method that
      // node-fetch's headers have.
      'raw' in response.headers
        ? nodeFetchHeadersToCachePolicyHeaders(response.headers)
        : Object.fromEntries(response.headers),
  };
}

// In the special case that these headers come from node-fetch, uses
// node-fetch's `raw()` method (which returns a `Record<string, string[]>`) to
// create our CachePolicy.Headers. Note that while we could theoretically just
// return `headers.raw()` here (it does match the typing of
// CachePolicy.Headers), `http-cache-semantics` sadly does expect most of the
// headers it pays attention to to only show up once (see eg
// https://github.com/kornelski/http-cache-semantics/issues/28). We want to
// preserve the multiplicity of other headers that CachePolicy doesn't parse
// (like set-cookie) because we store the CachePolicy in the cache, but not the
// interesting ones that we hope were singletons, so this function
// de-singletonizes singleton response headers.
function nodeFetchHeadersToCachePolicyHeaders(
  headers: NodeFetchHeaders,
): CachePolicy.Headers {
  const cachePolicyHeaders = Object.create(null);
  for (const [name, values] of Object.entries(headers.raw())) {
    cachePolicyHeaders[name] = values.length === 1 ? values[0] : values;
  }
  return cachePolicyHeaders;
}

// CachePolicy.Headers can store header values as string or string-array (for
// duplicate headers). Convert it to "list of pairs", which is a valid
// `node-fetch` constructor argument and will preserve the separation of
// duplicate headers.
// function cachePolicyHeadersToNodeFetchHeadersInit(
//   headers: CachePolicy.Headers,
// ): NodeFetchHeadersInit {
//   const headerList = [];
//   for (const [name, value] of Object.entries(headers)) {
//     if (Array.isArray(value)) {
//       for (const subValue of value) {
//         headerList.push([name, subValue]);
//       }
//     } else if (value) {
//       headerList.push([name, value]);
//     }
//   }
//   return headerList;
// }

// CachePolicy.Headers can store header values as string or string-array (for
// duplicate headers). Convert it to "Record of strings", which is all we allow
// for HeadersInit in Fetcher. (Perhaps we should expand that definition in
// `@apollo/utils.fetcher` to allow HeadersInit to be `string[][]` too; then we
// could use the same function as cachePolicyHeadersToNodeFetchHeadersInit here
// which would let us more properly support duplicate headers in *requests* if
// using node-fetch.)
function cachePolicyHeadersToFetcherHeadersInit(
  headers: CachePolicy.Headers,
): Record<string, string> {
  const headerRecord = Object.create(null);
  for (const [name, value] of Object.entries(headers)) {
    if (Array.isArray(value)) {
      headerRecord[name] = value.join(', ');
    } else if (value) {
      headerRecord[name] = value;
    }
  }
  return headerRecord;
}
