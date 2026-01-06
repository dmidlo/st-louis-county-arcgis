import asyncio
from stlouis_county_gis import StLouisCountyOpenDataClient

async def main():
    async with StLouisCountyOpenDataClient() as c:
        p = await c.list_parcels_first_page(page_size=5)
        a = await c.list_address_points_first_page(page_size=5)
        print("parcels:", len(p.features))
        print("addresses:", len(a.features))

        pn = p.features[0].attributes.get("PRCL_NBR")
        b = await c.parcel_bundle(parcel_number=str(pn), max_features_per_layer=200)
        print("bundle matches:", len(b.matches), "address points:", len(b.address_points))

asyncio.run(main())
