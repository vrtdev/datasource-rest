import fetch from 'node-fetch';
import nock from 'nock';
import { CACHE_ENTRY_STRING_MARSHALLER, HTTPCache } from '../HTTPCache';
import type { CacheOptions, RequestOptions } from '../RESTDataSource';
import { nockAfterEach, nockBeforeEach } from './nockAssertions';
import { FakeableTTLTestingCache } from './FakeableTTLTestingCache';
import type { FetcherResponse } from '@apollo/utils.fetcher';

interface CustomCacheOptions extends CacheOptions {
  tags?: string[];
}

describe('HTTPCache', () => {
  let store: FakeableTTLTestingCache;
  let httpCache: HTTPCache;

  beforeEach(() => {
    nockBeforeEach();
    store = new FakeableTTLTestingCache();
    httpCache = new HTTPCache(store, fetch, CACHE_ENTRY_STRING_MARSHALLER);
  });

  afterEach(nockAfterEach);

  beforeAll(() => {
    // nock depends on process.nextTick (and we use it to make async functions actually async)
    jest.useFakeTimers({ doNotFake: ['nextTick'] });
  });

  afterAll(() => {
    jest.useRealTimers();
  });

  const apiUrl = 'https://api.example.com';
  const adaPath = '/people/1';
  const adaUrl = new URL(`${apiUrl}${adaPath}`);

  function mockGetAdaLovelace(headers: { [key: string]: string } = {}) {
    return nock(apiUrl).get(adaPath).reply(
      200,
      {
        name: 'Ada Lovelace',
      },
      headers,
    );
  }

  function mockGetAlanTuring(headers: { [key: string]: string } = {}) {
    return nock(apiUrl).get(adaPath).reply(
      200,
      {
        name: 'Alan Turing',
      },
      headers,
    );
  }

  function mockInternalServerError(headers: { [key: string]: string } = {}) {
    return nock(apiUrl)
      .get(adaPath)
      .reply(500, 'Internal Server Error', headers);
  }

  const requestOpts: RequestOptions = {
    method: 'GET',
    headers: {},
    params: new URLSearchParams(),
  };

  const cache = {
    cacheKey: 'foo',
  };

  async function responseParser(response: FetcherResponse) {
    const headers: { [key: string]: string } = {};
    for (const [name, values] of response.headers) {
      headers[name] = values.length === 1 ? values[0] : values;
    }
    const json =
      response.ok && headers['content-type'] === 'application/json'
        ? await response.json()
        : '';
    return {
      result: {
        url: response.url,
        headers,
        status: response.status,
        json,
      },
    };
  }

  it('fetches a response from the origin when not cached', async () => {
    mockGetAdaLovelace();

    const { result, cacheWritePromise } = await httpCache.fetch(
      adaUrl,
      requestOpts,
      cache,
      responseParser,
    );
    expect(cacheWritePromise).toBeUndefined();

    expect(await result?.json).toEqual({ name: 'Ada Lovelace' });
  });

  it('returns a cached response when not expired', async () => {
    mockGetAdaLovelace({ 'cache-control': 'max-age=30' });

    const { result: firstResult, cacheWritePromise } = await httpCache.fetch(
      adaUrl,
      requestOpts,
      cache,
      responseParser,
    );
    expect(firstResult?.url).toBe(adaUrl.toString());

    await cacheWritePromise;
    jest.advanceTimersByTime(10000);

    const { result } = await httpCache.fetch(
      adaUrl,
      requestOpts,
      cache,
      responseParser,
    );

    expect(result?.url).toBe(adaUrl.toString());
    expect(await result?.json).toEqual({ name: 'Ada Lovelace' });
    // FIXME check through cache metrics
    //expect(result.headers['age']).toEqual('10');
  });

  it('fetches a fresh response from the origin when expired', async () => {
    mockGetAdaLovelace({ 'cache-control': 'max-age=30' });

    const { cacheWritePromise } = await httpCache.fetch(
      adaUrl,
      requestOpts,
      cache,
      responseParser,
    );

    await cacheWritePromise;
    jest.advanceTimersByTime(30000);

    mockGetAlanTuring({ 'cache-control': 'max-age=30' });

    const { result } = await httpCache.fetch(
      new URL('https://api.example.com/people/1'),
      requestOpts,
      cache,
      responseParser,
    );

    expect(await result?.json).toEqual({ name: 'Alan Turing' });
    // FIXME check through cache metrics
    //expect(result.headers['age']).toBeNull();
  });

  describe('overriding TTL', () => {
    it('returns a cached response when the overridden TTL is not expired', async () => {
      mockGetAdaLovelace({
        'cache-control': 'private, no-cache',
        'set-cookie': 'foo',
      });

      const cache = {
        cacheKey: adaUrl.toString(),
        cacheOptions: {
          ttl: 30,
        },
      };

      const { cacheWritePromise } = await httpCache.fetch(
        adaUrl,
        requestOpts,
        cache,
        responseParser,
      );

      await cacheWritePromise;
      jest.advanceTimersByTime(10000);

      const { result } = await httpCache.fetch(
        adaUrl,
        requestOpts,
        cache,
        responseParser,
      );

      expect(await result?.json).toEqual({ name: 'Ada Lovelace' });
      // FIXME check through cache metrics
      //expect(result.headers['age']).toEqual('10');
    });

    it('fetches a fresh response from the origin when the overridden TTL expired', async () => {
      mockGetAdaLovelace({
        'cache-control': 'private, no-cache',
        'set-cookie': 'foo',
      });

      const { cacheWritePromise } = await httpCache.fetch(
        adaUrl,
        requestOpts,
        {
          cacheKey: adaUrl.toString(),
          cacheOptions: {
            ttl: 30,
          },
        },
        responseParser,
      );

      await cacheWritePromise;
      jest.advanceTimersByTime(30000);

      mockGetAlanTuring({
        'cache-control': 'private, no-cache',
        'set-cookie': 'foo',
      });

      const { result } = await httpCache.fetch(
        new URL('https://api.example.com/people/1'),
        requestOpts,
        cache,
        responseParser,
      );

      expect(await result?.json).toEqual({ name: 'Alan Turing' });
      // FIXME check through cache metrics
      //expect(result.headers['age']).toBeNull();
    });

    it('fetches a fresh response from the origin when the overridden TTL expired even if a longer max-age has been specified', async () => {
      mockGetAdaLovelace({ 'cache-control': 'max-age=30' });

      const { cacheWritePromise } = await httpCache.fetch(
        adaUrl,
        requestOpts,
        {
          cacheKey: adaUrl.toString(),
          cacheOptions: {
            ttl: 10,
          },
        },
        responseParser,
      );

      await cacheWritePromise;
      jest.advanceTimersByTime(10000);

      mockGetAlanTuring({
        'cache-control': 'private, no-cache',
      });

      const { result } = await httpCache.fetch(
        new URL('https://api.example.com/people/1'),
        requestOpts,
        cache,
        responseParser,
      );

      expect(await result?.json).toEqual({ name: 'Alan Turing' });
      // FIXME check through cache metrics
      //expect(result.headers['age']).toBeNull();
    });

    it('does not store a response with an overridden TTL and a non-success status code', async () => {
      mockInternalServerError({ 'cache-control': 'max-age=30' });

      const cache = {
        cacheKey: adaUrl.toString(),
        cacheOptions: {
          ttl: 30,
        },
      };

      const { cacheWritePromise } = await httpCache.fetch(
        adaUrl,
        requestOpts,
        cache,
        responseParser,
      );

      expect(cacheWritePromise).toBeUndefined();
      expect(store.isEmpty()).toBe(true);
    });

    it('allows overriding the TTL dynamically', async () => {
      mockGetAdaLovelace({
        'cache-control': 'private, no-cache',
        'set-cookie': 'foo',
      });

      const cache = {
        cacheKey: adaUrl.toString(),
        cacheOptions: () => ({
          ttl: 30,
        }),
      };

      const { cacheWritePromise } = await httpCache.fetch(
        adaUrl,
        requestOpts,
        cache,
        responseParser,
      );

      await cacheWritePromise;
      jest.advanceTimersByTime(10000);

      const { result } = await httpCache.fetch(
        adaUrl,
        requestOpts,
        cache,
        responseParser,
      );

      expect(await result?.json).toEqual({ name: 'Ada Lovelace' });
      // FIXME check through cache metrics
      //expect(result.headers['age']).toEqual('10');
    });

    it('allows overriding the TTL dynamically with an async function', async () => {
      mockGetAdaLovelace({
        'cache-control': 'private, no-cache',
        'set-cookie': 'foo',
      });
      const cache = {
        cacheKey: adaUrl.toString(),
        cacheOptions: async () => {
          // Make it really async (using nextTick because we're not mocking it)
          await new Promise<void>((resolve) => process.nextTick(resolve));
          return {
            ttl: 30,
          };
        },
      };
      const { cacheWritePromise } = await httpCache.fetch(
        adaUrl,
        requestOpts,
        cache,
        responseParser,
      );

      await cacheWritePromise;
      jest.advanceTimersByTime(10000);

      const { result } = await httpCache.fetch(
        adaUrl,
        requestOpts,
        cache,
        responseParser,
      );

      expect(await result?.json).toEqual({ name: 'Ada Lovelace' });
      // FIXME check through cache metrics
      //expect(result.headers['age']).toEqual('10');
    });

    it('allows disabling caching when the TTL is 0 (falsy)', async () => {
      mockGetAdaLovelace({ 'cache-control': 'max-age=30' });

      const { cacheWritePromise } = await httpCache.fetch(
        adaUrl,
        requestOpts,
        {
          cacheKey: adaUrl.toString(),
          cacheOptions: () => ({
            ttl: 0,
          }),
        },
        responseParser,
      );

      expect(cacheWritePromise).toBeUndefined();
      expect(store.isEmpty()).toBe(true);
    });
  });

  it('allows specifying a custom cache key', async () => {
    nock(apiUrl)
      .get(adaPath)
      .query({ foo: '123' })
      .reply(200, { name: 'Ada Lovelace' }, { 'cache-control': 'max-age=30' });

    const { cacheWritePromise } = await httpCache.fetch(
      new URL(`${adaUrl}?foo=123`),
      requestOpts,
      { cacheKey: adaUrl.toString() },
      responseParser,
    );

    await cacheWritePromise;
    const { result } = await httpCache.fetch(
      new URL(`${adaUrl}?foo=456`),
      requestOpts,
      { cacheKey: adaUrl.toString() },
      responseParser,
    );

    expect(await result?.json).toEqual({ name: 'Ada Lovelace' });
  });

  it('does not store a response to a non-GET/HEAD request', async () => {
    nock(apiUrl)
      .post(adaPath)
      .reply(200, { name: 'Ada Lovelace' }, { 'cache-control': 'max-age=30' });

    const { cacheWritePromise } = await httpCache.fetch(
      adaUrl,
      {
        ...requestOpts,
        method: 'POST',
      },
      { cacheKey: adaUrl.toString() },
      responseParser,
    );

    expect(cacheWritePromise).toBeUndefined();
    expect(store.isEmpty()).toBe(true);
  });

  it('does not store a response with a non-success status code', async () => {
    mockInternalServerError({ 'cache-control': 'max-age=30' });

    const { cacheWritePromise } = await httpCache.fetch(
      adaUrl,
      requestOpts,
      cache,
      responseParser,
    );

    expect(cacheWritePromise).toBeUndefined();
    expect(store.isEmpty()).toBe(true);
  });

  it('does not store a response without cache-control header', async () => {
    mockGetAdaLovelace();

    const { cacheWritePromise } = await httpCache.fetch(
      adaUrl,
      requestOpts,
      cache,
      responseParser,
    );

    expect(cacheWritePromise).toBeUndefined();
    expect(store.isEmpty()).toBe(true);
  });

  it('does not store a private response', async () => {
    mockGetAdaLovelace({ 'cache-control': 'private, max-age: 60' });

    const { cacheWritePromise } = await httpCache.fetch(
      adaUrl,
      requestOpts,
      cache,
      responseParser,
    );

    expect(cacheWritePromise).toBeUndefined();
    expect(store.isEmpty()).toBe(true);
  });

  it('returns a cached response when vary header fields match', async () => {
    mockGetAdaLovelace({
      'cache-control': 'max-age=30',
      vary: 'Accept-Language',
    });

    const { cacheWritePromise } = await httpCache.fetch(
      adaUrl,
      {
        ...requestOpts,
        headers: { 'accept-language': 'en' },
      },
      cache,
      responseParser,
    );
    await cacheWritePromise;

    const { result } = await httpCache.fetch(
      adaUrl,
      {
        ...requestOpts,
        headers: { 'accept-language': 'en' },
      },
      cache,
      responseParser,
    );

    expect(await result?.json).toEqual({ name: 'Ada Lovelace' });
  });

  it(`does not return a cached response when vary header fields don't match`, async () => {
    mockGetAdaLovelace({
      'cache-control': 'max-age=30',
      vary: 'Accept-Language',
    });

    const { cacheWritePromise } = await httpCache.fetch(
      adaUrl,
      {
        ...requestOpts,
        headers: { 'accept-language': 'en' },
      },
      cache,
      responseParser,
    );
    await cacheWritePromise;

    mockGetAlanTuring({ 'cache-control': 'max-age=30' });

    const { result } = await httpCache.fetch(
      new URL('https://api.example.com/people/1'),
      {
        ...requestOpts,
        headers: { 'accept-language': 'fr' },
      },
      cache,
      responseParser,
    );

    expect(await result?.json).toEqual({ name: 'Alan Turing' });
  });

  it('sets the TTL as max-age when the response does not contain revalidation headers', async () => {
    mockGetAdaLovelace({ 'cache-control': 'max-age=30' });

    const storeSet = jest.spyOn(store, 'set');

    const { cacheWritePromise } = await httpCache.fetch(
      adaUrl,
      requestOpts,
      cache,
      responseParser,
    );
    await cacheWritePromise;

    expect(storeSet).toHaveBeenCalledWith(
      expect.any(String),
      expect.any(String),
      { ttl: 30 },
    );
    storeSet.mockRestore();
  });

  it('sets the TTL as 2 * max-age when the response contains an ETag header', async () => {
    mockGetAdaLovelace({ 'cache-control': 'max-age=30', etag: 'foo' });

    const storeSet = jest.spyOn(store, 'set');

    const { cacheWritePromise } = await httpCache.fetch(
      adaUrl,
      requestOpts,
      cache,
      responseParser,
    );
    await cacheWritePromise;

    expect(storeSet).toHaveBeenCalledWith(
      expect.any(String),
      expect.any(String),
      { ttl: 60 },
    );

    storeSet.mockRestore();
  });

  it('sets the TTL less than max-age when the response contains an ETag header, and set additional cache options', async () => {
    mockGetAdaLovelace({ 'cache-control': 'max-age=30' });

    const storeSet = jest.spyOn(store, 'set');

    const customHttpCache = new HTTPCache<CustomCacheOptions>(
      store,
      fetch,
      CACHE_ENTRY_STRING_MARSHALLER,
    );
    const { cacheWritePromise } = await customHttpCache.fetch(
      adaUrl,
      requestOpts,
      {
        cacheKey: adaUrl.toString(),
        cacheOptions: {
          ttl: 20,
          tags: ['foo', 'bar'],
        },
      },
      responseParser,
    );
    await cacheWritePromise;

    expect(storeSet).toHaveBeenCalledWith(
      expect.any(String),
      expect.any(String),
      { ttl: 20, tags: ['foo', 'bar'] },
    );

    storeSet.mockRestore();
  });

  it('revalidates a cached response when expired and returns the cached response when not modified via etag', async () => {
    mockGetAdaLovelace({
      'cache-control': 'public, max-age=30',
      etag: 'foo',
    });

    const { result: result0, cacheWritePromise: cwp1 } = await httpCache.fetch(
      adaUrl,
      requestOpts,
      cache,
      responseParser,
    );
    expect(result0?.status).toEqual(200);
    expect(await result0?.json).toEqual({ name: 'Ada Lovelace' });
    // FIXME check through cache metrics
    //expect(result0.headers['age']).toBeNull();

    await cwp1;
    jest.advanceTimersByTime(10000);

    const { result: result1, cacheWritePromise: cwp2 } = await httpCache.fetch(
      adaUrl,
      requestOpts,
      cache,
      responseParser,
    );
    expect(cwp2).toBeUndefined();
    expect(result1?.status).toEqual(200);
    expect(await result1?.json).toEqual({ name: 'Ada Lovelace' });
    // FIXME check through cache metrics
    //expect(result1.headers['age']).toEqual('10');

    jest.advanceTimersByTime(21000);

    nock(apiUrl)
      .get(adaPath)
      .matchHeader('if-none-match', 'foo')
      .reply(304, undefined, {
        'cache-control': 'public, max-age=30',
        etag: 'foo',
      });

    const { result, cacheWritePromise: cwp3 } = await httpCache.fetch(
      adaUrl,
      requestOpts,
      cache,
      responseParser,
    );
    expect(result?.status).toEqual(200);
    expect(await result?.json).toEqual({ name: 'Ada Lovelace' });
    // FIXME check through cache metrics
    //expect(result.headers['age']).toEqual('0');

    await cwp3;
    jest.advanceTimersByTime(10000);

    const { result: result2, cacheWritePromise: cwp4 } = await httpCache.fetch(
      adaUrl,
      requestOpts,
      cache,
      responseParser,
    );

    expect(cwp4).toBeUndefined();
    expect(result2?.status).toEqual(200);
    expect(await result2?.json).toEqual({ name: 'Ada Lovelace' });
    // FIXME check through cache metrics
    //expect(result2.headers['age']).toEqual('10');
  });

  it('revalidates a cached response when expired and returns the cached response when not modified via last-modified', async () => {
    mockGetAdaLovelace({
      'cache-control': 'public, max-age=30',
      'last-modified': 'Wed, 21 Oct 2015 07:28:00 GMT',
    });

    const { result: result0, cacheWritePromise: cwp1 } = await httpCache.fetch(
      adaUrl,
      requestOpts,
      cache,
      responseParser,
    );
    expect(result0?.status).toEqual(200);
    expect(await result0?.json).toEqual({ name: 'Ada Lovelace' });
    // FIXME check through cache metrics
    //expect(result0.headers['age']).toBeNull();

    await cwp1;
    jest.advanceTimersByTime(10000);

    const { result: result1, cacheWritePromise: cwp2 } = await httpCache.fetch(
      adaUrl,
      requestOpts,
      cache,
      responseParser,
    );
    expect(cwp2).toBeUndefined();
    expect(result1?.status).toEqual(200);
    expect(await result1?.json).toEqual({ name: 'Ada Lovelace' });
    // FIXME check through cache metrics
    //expect(result1.headers['age']).toEqual('10');

    jest.advanceTimersByTime(21000);

    nock(apiUrl)
      .get(adaPath)
      .matchHeader('if-modified-since', 'Wed, 21 Oct 2015 07:28:00 GMT')
      .reply(304, undefined, {
        'cache-control': 'public, max-age=30',
        'last-modified': 'Wed, 21 Oct 2015 07:28:00 GMT',
      });

    const { result, cacheWritePromise: cwp3 } = await httpCache.fetch(
      adaUrl,
      requestOpts,
      cache,
      responseParser,
    );

    expect(result?.status).toEqual(200);
    expect(await result?.json).toEqual({ name: 'Ada Lovelace' });
    // FIXME check through cache metrics
    //expect(result.headers['age']).toEqual('0');

    await cwp3;
    jest.advanceTimersByTime(10000);

    const { result: result2, cacheWritePromise: cwp4 } = await httpCache.fetch(
      adaUrl,
      requestOpts,
      cache,
      responseParser,
    );

    expect(cwp4).toBeUndefined();
    expect(result2?.status).toEqual(200);
    expect(await result2?.json).toEqual({ name: 'Ada Lovelace' });
    // FIXME check through cache metrics
    //expect(result2.headers['age']).toEqual('10');
  });

  it('revalidates a cached response when expired and returns and caches a fresh response when modified', async () => {
    mockGetAdaLovelace({
      'cache-control': 'public, max-age=30',
      etag: 'foo',
    });

    {
      const { cacheWritePromise } = await httpCache.fetch(
        adaUrl,
        requestOpts,
        cache,
        responseParser,
      );
      await cacheWritePromise;
    }

    jest.advanceTimersByTime(30000);

    mockGetAlanTuring({
      'cache-control': 'public, max-age=30',
      etag: 'bar',
    });

    {
      const { result, cacheWritePromise } = await httpCache.fetch(
        new URL('https://api.example.com/people/1'),
        requestOpts,
        cache,
        responseParser,
      );

      expect(cacheWritePromise).toBeDefined();
      expect(result?.status).toEqual(200);
      expect(await result?.json).toEqual({ name: 'Alan Turing' });

      await cacheWritePromise;
    }

    jest.advanceTimersByTime(10000);

    {
      const { result: result2, cacheWritePromise } = await httpCache.fetch(
        new URL('https://api.example.com/people/1'),
        requestOpts,
        cache,
        responseParser,
      );

      expect(cacheWritePromise).toBeUndefined();
      expect(result2?.status).toEqual(200);
      expect(await result2?.json).toEqual({ name: 'Alan Turing' });
      // FIXME check through cache metrics
      //expect(result2.headers['age']).toEqual('10');
    }
  });

  it('fetches a response from the origin with a custom fetch function', async () => {
    mockGetAdaLovelace();

    const customFetch = jest.fn(fetch);
    const customHttpCache = new HTTPCache(
      store,
      customFetch,
      CACHE_ENTRY_STRING_MARSHALLER,
    );

    const { result, cacheWritePromise } = await customHttpCache.fetch(
      adaUrl,
      requestOpts,
      cache,
      responseParser,
    );

    expect(cacheWritePromise).toBeUndefined();
    expect(await result?.json).toEqual({ name: 'Ada Lovelace' });
  });

  describe('HEAD requests', () => {
    it('bypasses the cache', async () => {
      // x2
      nock(apiUrl).head(adaPath).times(2).reply(200);

      for (const _ of [1, 2]) {
        const { cacheWritePromise } = await httpCache.fetch(
          adaUrl,
          {
            ...requestOpts,
            method: 'HEAD',
          },
          cache,
          responseParser,
        );
        expect(cacheWritePromise).toBeUndefined();
      }
    });

    it('bypasses the cache even with explicit ttl', async () => {
      // x2
      nock(apiUrl).head(adaPath).times(2).reply(200);

      {
        const { cacheWritePromise } = await httpCache.fetch(
          adaUrl,
          {
            ...requestOpts,
            method: 'HEAD',
          },
          {
            cacheKey: adaUrl.toString(),
            cacheOptions: { ttl: 30000 },
          },
          responseParser,
        );
        expect(cacheWritePromise).toBeUndefined();
      }

      {
        const { cacheWritePromise } = await httpCache.fetch(
          adaUrl,
          {
            ...requestOpts,
            method: 'HEAD',
          },
          cache,
          responseParser,
        );
        expect(cacheWritePromise).toBeUndefined();
      }
    });
  });
});
