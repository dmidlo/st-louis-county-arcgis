# stlouis-county-gis

Async ArcGIS REST client for St. Louis County (MN) Open_Data MapServer.

## Quick usage

```python
import asyncio
from stlouis_county_gis import StLouisCountyOpenDataClient

async def main():
    async with StLouisCountyOpenDataClient() as c:
        parcels = await c.list_parcels_first_page(page_size=25)
        print("parcels:", len(parcels.features))

        # iterate all parcels (careful: large)
        # async for f in c.iter_all(layer_id=(await c.find_layer_id_by_name_contains("Parcels"))):
        #     ...

        # bundle
        sample = parcels.features[0].attributes
        pn = sample.get("PRCL_NBR")
        if pn:
            b = await c.parcel_bundle(parcel_number=str(pn), max_features_per_layer=500)
            print("bundle matches:", len(b.matches))

asyncio.run(main())
