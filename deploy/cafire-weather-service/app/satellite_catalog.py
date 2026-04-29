from __future__ import annotations

from typing import Any


SATELLITE_PRODUCT_CATALOG: list[dict[str, Any]] = [
    {
        "product": "goes_geocolor",
        "name": "GeoColor",
        "description": "True Color daytime, multispectral IR at night",
        "category": "rgb",
    },
    {
        "product": "goes_glm_fed_geocolor",
        "name": "GLM FED3+GeoColor",
        "description": "Lightning flash extent over GeoColor",
        "category": "rgb",
    },
    {
        "product": "goes_airmass_rgb",
        "name": "AirMass RGB",
        "description": "RGB based on data from IR & water vapor",
        "category": "rgb",
    },
    {
        "product": "goes_sandwich_rgb",
        "name": "Sandwich RGB",
        "description": "Blend combines IR band 13 with visual band 3",
        "category": "rgb",
    },
    {
        "product": "goes_day_night_cloud_micro_combo_rgb",
        "name": "Day Night Cloud Micro Combo RGB",
        "description": "Day: show cloud-top phase; Night: distinguish clouds / fog",
        "category": "rgb",
    },
    {
        "product": "goes_fire_temperature_rgb",
        "name": "Fire Temperature",
        "description": "RGB used to highlight fires",
        "category": "rgb",
    },
    {
        "product": "goes_dust_rgb",
        "name": "Dust RGB",
        "description": "RGB for identifying tropospheric dust",
        "category": "rgb",
    },
    {
        "product": "goes_abi_band_01",
        "name": "Band 1",
        "wavelength": "0.47 µm",
        "description": "Blue - Visible",
        "category": "band",
    },
    {
        "product": "goes_abi_band_02",
        "name": "Band 2",
        "wavelength": "0.64 µm",
        "description": "Red - Visible",
        "category": "band",
    },
    {
        "product": "goes_abi_band_03",
        "name": "Band 3",
        "wavelength": "0.86 µm",
        "description": "Veggie - Near IR",
        "category": "band",
    },
    {
        "product": "goes_abi_band_04",
        "name": "Band 4",
        "wavelength": "1.37 µm",
        "description": "Cirrus - Near IR",
        "category": "band",
    },
    {
        "product": "goes_abi_band_05",
        "name": "Band 5",
        "wavelength": "1.6 µm",
        "description": "Snow/Ice - Near IR",
        "category": "band",
    },
    {
        "product": "goes_abi_band_06",
        "name": "Band 6",
        "wavelength": "2.2 µm",
        "description": "Cloud Particle - Near IR",
        "category": "band",
    },
    {
        "product": "goes_abi_band_07",
        "name": "Band 7",
        "wavelength": "3.9 µm",
        "description": "Shortwave Window - IR",
        "category": "band",
    },
    {
        "product": "goes_abi_band_08",
        "name": "Band 8",
        "wavelength": "6.2 µm",
        "description": "Upper-Level Water Vapor - IR",
        "category": "band",
    },
    {
        "product": "goes_abi_band_09",
        "name": "Band 9",
        "wavelength": "6.9 µm",
        "description": "Mid-Level Water Vapor - IR",
        "category": "band",
    },
    {
        "product": "goes_abi_band_10",
        "name": "Band 10",
        "wavelength": "7.3 µm",
        "description": "Lower-level Water Vapor - IR",
        "category": "band",
    },
    {
        "product": "goes_abi_band_11",
        "name": "Band 11",
        "wavelength": "8.4 µm",
        "description": "Cloud Top - IR",
        "category": "band",
    },
    {
        "product": "goes_abi_band_12",
        "name": "Band 12",
        "wavelength": "9.6 µm",
        "description": "Ozone - IR",
        "category": "band",
    },
    {
        "product": "goes_abi_band_13",
        "name": "Band 13",
        "wavelength": "10.3 µm",
        "description": "Clean Longwave Window - IR",
        "category": "band",
    },
    {
        "product": "goes_abi_band_14",
        "name": "Band 14",
        "wavelength": "11.2 µm",
        "description": "Longwave Window - IR",
        "category": "band",
    },
    {
        "product": "goes_abi_band_15",
        "name": "Band 15",
        "wavelength": "12.3 µm",
        "description": "Dirty Longwave Window - IR",
        "category": "band",
    },
    {
        "product": "goes_abi_band_16",
        "name": "Band 16",
        "wavelength": "13.3 µm",
        "description": "CO₂ Longwave - IR",
        "category": "band",
    },
]


def product_catalog_for(products: list[str]) -> list[dict[str, Any]]:
    by_product = {item["product"]: {**item, "display_order": index} for index, item in enumerate(SATELLITE_PRODUCT_CATALOG)}
    result: list[dict[str, Any]] = []
    for index, product in enumerate(products):
        item = by_product.get(product)
        if item is None:
            item = {
                "product": product,
                "name": product.replace("_", " "),
                "description": "",
                "category": "other",
                "display_order": index,
            }
        result.append(dict(item))
    return result


def product_metadata(product: str, products: list[str] | None = None) -> dict[str, Any]:
    source = products or [item["product"] for item in SATELLITE_PRODUCT_CATALOG]
    catalog = product_catalog_for(source)
    for item in catalog:
        if item["product"] == product:
            return dict(item)
    return {
        "product": product,
        "name": product.replace("_", " "),
        "description": "",
        "category": "other",
        "display_order": len(catalog),
    }
