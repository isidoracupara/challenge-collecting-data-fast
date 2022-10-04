import httpx
import asyncio
import itertools
import time

def get_price_range(minPrice, maxPrice, session):
    api_url = f"https://www.immoweb.be/en/search-results/house-and-apartment/for-sale?countries=BE&page=1&orderBy=newest&isALifeAnnuitySale=false&minPrice={minPrice}&maxPrice={maxPrice}"
    num_pages = session.get(api_url).json()['marketingCount'] // 30 + 1
    if num_pages > 333:
        mid_price = (minPrice + maxPrice) // 2 
        return get_price_range(minPrice, mid_price, session) + get_price_range(mid_price, maxPrice, session)
    return [(minPrice, maxPrice, num_pages)]

async def get_ids_from_page(i, minPrice, maxPrice, session):
    api_url = f"https://www.immoweb.be/en/search-results/house-and-apartment/for-sale?countries=BE&page={i}&orderBy=newest&isALifeAnnuitySale=false&minPrice={minPrice}&maxPrice={maxPrice}"
    res = await session.get(api_url, timeout=5)
    
    return [result['id'] for result in res.json()['results']]

async def get_ids_for_category(minPrice, maxPrice, num_pages):
    session = httpx.AsyncClient()
    jobs = [asyncio.create_task(get_ids_from_page(i, minPrice, maxPrice, session)) for i in range(1, num_pages+ 1)]
    res = list(await asyncio.gather(*jobs))
    await session.aclose()
    return res

with httpx.Client(timeout=10) as session:
    price_ranges = get_price_range(0, 1000000, session)

start = time.time()
ids = set()
for minPrice, maxPrice, num_pages in price_ranges:
    ids.update(set(itertools.chain.from_iterable(asyncio.run(get_ids_for_category(minPrice, maxPrice, num_pages)))))

end = time.time()
print(f"Found {len(ids)} ids in {end - start} seconds")