"""Radar site metadata helpers for RustWx Studio.

The site rows mirror ``crates/rustwx-radar/src/nexrad/sites.rs`` so the Python
UI can paint clickable radar points without needing a separate Rust binding.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable

SOURCE = "crates/rustwx-radar/src/nexrad/sites.rs"

LOWER_48_STATES = frozenset(
    {
        "AL",
        "AR",
        "AZ",
        "CA",
        "CO",
        "CT",
        "DE",
        "FL",
        "GA",
        "IA",
        "ID",
        "IL",
        "IN",
        "KS",
        "KY",
        "LA",
        "MA",
        "MD",
        "ME",
        "MI",
        "MN",
        "MO",
        "MS",
        "MT",
        "NC",
        "ND",
        "NE",
        "NH",
        "NJ",
        "NM",
        "NV",
        "NY",
        "OH",
        "OK",
        "OR",
        "PA",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VA",
        "VT",
        "WA",
        "WI",
        "WV",
        "WY",
    }
)


@dataclass(frozen=True)
class RadarSite:
    id: str
    name: str
    city: str
    state: str
    lat: float
    lon: float
    network: str
    type: str
    conus: bool
    lower_48: bool

    @property
    def label(self) -> str:
        return f"{self.id} - {self.city}, {self.state}"

    def to_json(self) -> dict[str, object]:
        payload = asdict(self)
        payload["label"] = self.label
        return payload


_RADAR_SITE_ROWS = """
KABR|Aberdeen|SD|45.4558|-98.4131
KABX|Albuquerque|NM|35.1497|-106.824
KAKQ|Wakefield|VA|36.9839|-77.0075
KAMA|Amarillo|TX|35.2333|-101.709
KAMX|Miami|FL|25.6111|-80.4128
KAPX|Gaylord|MI|44.9072|-84.7197
KARX|La Crosse|WI|43.8228|-91.1911
KATX|Seattle|WA|48.1944|-122.496
KBBX|Beale AFB|CA|39.4961|-121.632
KBGM|Binghamton|NY|42.1997|-75.985
KBHX|Eureka|CA|40.4986|-124.292
KBIS|Bismarck|ND|46.7708|-100.76
KBLX|Billings|MT|45.8536|-108.607
KBMX|Birmingham|AL|33.1722|-86.7697
KBOX|Boston|MA|41.9558|-71.1369
KBRO|Brownsville|TX|25.9161|-97.4189
KBUF|Buffalo|NY|42.9486|-78.7369
KBYX|Key West|FL|24.5975|-81.7031
KCAE|Columbia|SC|33.9486|-81.1186
KCBW|Caribou|ME|46.0392|-67.8064
KCBX|Boise|ID|43.4908|-116.236
KCCX|State College|PA|40.9228|-78.0039
KCLE|Cleveland|OH|41.4131|-81.86
KCLX|Charleston|SC|32.6556|-81.0422
KCRP|Corpus Christi|TX|27.7842|-97.5111
KCXX|Burlington|VT|44.5111|-73.1667
KCYS|Cheyenne|WY|41.1519|-104.806
KDAX|Sacramento|CA|38.5011|-121.678
KDDC|Dodge City|KS|37.7608|-99.9689
KDFX|Laughlin AFB|TX|29.2725|-100.281
KDGX|Brandon|MS|32.28|-89.9844
KDIX|Philadelphia|NJ|39.9469|-74.4108
KDLH|Duluth|MN|46.8369|-92.2097
KDMX|Des Moines|IA|41.7311|-93.7228
KDOX|Dover AFB|DE|38.8256|-75.44
KDTX|Detroit|MI|42.6997|-83.4717
KDVN|Davenport|IA|41.6117|-90.5808
KDYX|Dyess AFB|TX|32.5386|-99.2542
KEAX|Kansas City|MO|38.8103|-94.2644
KEMX|Tucson|AZ|31.8936|-110.63
KENX|Albany|NY|42.5864|-74.0639
KEOX|Fort Rucker|AL|31.4606|-85.4594
KEPZ|El Paso|TX|31.8731|-106.698
KESX|Las Vegas|NV|35.7011|-114.892
KEVX|NW Florida|FL|30.5644|-85.9214
KEWX|Austin/San Antonio|TX|29.7039|-98.0286
KEYX|Edwards AFB|CA|35.0978|-117.561
KFCX|Roanoke|VA|37.0242|-80.2742
KFDR|Frederick|OK|34.3622|-98.9764
KFDX|Cannon AFB|NM|34.6353|-103.631
KFFC|Atlanta|GA|33.3636|-84.5658
KFSD|Sioux Falls|SD|43.5878|-96.7292
KFSX|Flagstaff|AZ|34.5744|-111.198
KFTG|Denver|CO|39.7867|-104.546
KFWS|Dallas/Ft Worth|TX|32.5731|-97.3028
KGGW|Glasgow|MT|48.2064|-106.625
KGJX|Grand Junction|CO|39.0619|-108.214
KGLD|Goodland|KS|39.3667|-101.7
KGRB|Green Bay|WI|44.4986|-88.1111
KGRK|Central Texas|TX|30.7217|-97.3828
KGRR|Grand Rapids|MI|42.8939|-85.5447
KGSP|Greenville|SC|34.8833|-82.2200
KGWX|Columbus AFB|MS|33.8967|-88.3289
KGYX|Portland|ME|43.8914|-70.2564
KHDX|Holloman AFB|NM|33.0769|-106.123
KHGX|Houston|TX|29.4719|-95.0792
KHNX|Hanford|CA|36.3142|-119.632
KHPX|Fort Campbell|KY|36.7369|-87.285
KHTX|Huntsville|AL|34.9306|-86.0833
KICT|Wichita|KS|37.6544|-97.4428
KICX|Cedar City|UT|37.5908|-112.862
KILN|Cincinnati|OH|39.4203|-83.8217
KILX|Lincoln|IL|40.1506|-89.3369
KIND|Indianapolis|IN|39.7075|-86.2803
KINX|Tulsa|OK|36.175|-95.5644
KIWA|Phoenix|AZ|33.2892|-111.67
KIWX|N Indiana|IN|41.3586|-85.7
KJAX|Jacksonville|FL|30.4847|-81.7019
KJGX|Robins AFB|GA|32.675|-83.3511
KJKL|Jackson|KY|37.5908|-83.3131
KLBB|Lubbock|TX|33.6542|-101.814
KLCH|Lake Charles|LA|30.125|-93.2158
KLIX|New Orleans|LA|30.3367|-89.8256
KLNX|North Platte|NE|41.9578|-100.576
KLOT|Chicago|IL|41.6044|-88.0847
KLRX|Elko|NV|40.7397|-116.803
KLSX|St. Louis|MO|38.6986|-90.6828
KLTX|Wilmington|NC|33.9892|-78.4292
KLVX|Louisville|KY|37.975|-85.9436
KLWX|Sterling|VA|38.9753|-77.4778
KLZK|Little Rock|AR|34.8364|-92.2622
KMAF|Midland/Odessa|TX|31.9433|-102.189
KMAX|Medford|OR|42.0811|-122.717
KMBX|Minot AFB|ND|48.3925|-100.864
KMHX|Morehead City|NC|34.7761|-76.8761
KMKX|Milwaukee|WI|42.9678|-88.5506
KMLB|Melbourne|FL|28.1133|-80.6542
KMOB|Mobile|AL|30.6794|-88.2397
KMPX|Minneapolis|MN|44.8489|-93.5653
KMQT|Marquette|MI|46.5311|-87.5486
KMRX|Knoxville|TN|36.1686|-83.4017
KMSX|Missoula|MT|47.0411|-113.986
KMTX|Salt Lake City|UT|41.2628|-112.448
KMUX|San Francisco|CA|37.1553|-121.898
KMVX|Fargo|ND|47.5278|-97.325
KNKX|San Diego|CA|32.9189|-117.042
KNQA|Memphis|TN|35.3447|-89.8733
KOAX|Omaha|NE|41.3203|-96.3669
KOHX|Nashville|TN|36.2472|-86.5625
KOKX|New York City|NY|40.8656|-72.8639
KOTX|Spokane|WA|47.6806|-117.627
KPAH|Paducah|KY|37.0683|-88.7719
KPBZ|Pittsburgh|PA|40.5317|-80.0178
KPDT|Pendleton|OR|45.6906|-118.853
KPOE|Fort Polk|LA|31.1556|-92.9758
KPUX|Pueblo|CO|38.4594|-104.181
KRAX|Raleigh|NC|35.6653|-78.49
KRGX|Reno|NV|39.7542|-119.462
KRIW|Riverton|WY|43.0661|-108.477
KRLX|Charleston|WV|38.3111|-81.7228
KRTX|Portland|OR|45.715|-122.965
KSFX|Pocatello|ID|43.1058|-112.686
KSGF|Springfield|MO|37.235|-93.4006
KSHV|Shreveport|LA|32.4508|-93.8414
KSJT|San Angelo|TX|31.3711|-100.493
KSOX|Santa Ana Mtns|CA|33.8178|-117.636
KSRX|Fort Smith|AR|35.2906|-94.3619
KTBW|Tampa Bay|FL|27.7056|-82.4017
KTFX|Great Falls|MT|47.4597|-111.385
KTLH|Tallahassee|FL|30.3975|-84.3289
KTLX|Oklahoma City|OK|35.3331|-97.2778
KTWX|Topeka|KS|38.9969|-96.2325
KTYX|Montague|NY|43.7556|-75.68
KUDX|Rapid City|SD|44.125|-102.83
KUEX|Hastings|NE|40.3208|-98.4419
KVAX|Moody AFB|GA|30.8903|-83.0019
KVBX|Vandenberg AFB|CA|34.8383|-120.398
KVNX|Vance AFB|OK|36.7408|-98.1275
KVTX|Los Angeles|CA|34.4117|-119.179
KVWX|Evansville|IN|38.2603|-87.7247
KYUX|Yuma|AZ|32.4953|-114.657
KCRI|Norman|OK|35.2383|-97.4602
KLGX|Langley Hill|WA|47.1169|-124.1069
KMXX|E Alabama|AL|32.5367|-85.7897
PACG|Juneau|AK|56.8527|-135.529
PAEC|Nome|AK|64.5114|-165.295
PAHG|Anchorage|AK|60.7258|-151.351
PAIH|Middleton Island|AK|59.4614|-146.303
PAKC|King Salmon|AK|58.6794|-156.629
PAPD|Fairbanks|AK|65.0356|-147.502
PHKI|South Kauai|HI|21.8942|-159.552
PHKM|Kohala|HI|20.1253|-155.778
PHMO|Molokai|HI|21.1328|-157.18
PHWA|South Shore|HI|19.095|-155.569
PGUA|Andersen AFB|GU|13.455|144.8111
TJUA|San Juan|PR|18.1156|-66.0783
RODN|Kadena|OK|26.3019|127.9094
""".strip()


def _is_lower_48(state: str, lat: float, lon: float) -> bool:
    return state in LOWER_48_STATES and 24.0 <= lat <= 50.0 and -125.5 <= lon <= -66.0


def _parse_sites(rows: str) -> tuple[RadarSite, ...]:
    sites: list[RadarSite] = []
    for raw_row in rows.splitlines():
        site_id, city, state, lat_raw, lon_raw = raw_row.split("|")
        lat = float(lat_raw)
        lon = float(lon_raw)
        lower_48 = _is_lower_48(state, lat, lon)
        sites.append(
            RadarSite(
                id=site_id,
                name=f"{city}, {state}",
                city=city,
                state=state,
                lat=lat,
                lon=lon,
                network="NEXRAD",
                type="WSR-88D",
                conus=lower_48,
                lower_48=lower_48,
            )
        )
    return tuple(sites)


RADAR_SITES = _parse_sites(_RADAR_SITE_ROWS)
_RADAR_SITES_BY_ID = {site.id: site for site in RADAR_SITES}


def all_radar_sites() -> list[dict[str, object]]:
    return [site.to_json() for site in RADAR_SITES]


def conus_radar_sites() -> list[dict[str, object]]:
    return radar_sites_json(conus=True)


def radar_sites_json(*, conus: bool | None = True) -> list[dict[str, object]]:
    sites: Iterable[RadarSite] = RADAR_SITES
    if conus is True:
        sites = (site for site in sites if site.conus)
    elif conus is False:
        sites = (site for site in sites if not site.conus)
    return [site.to_json() for site in sites]


def radar_sites_geojson(*, conus: bool | None = True) -> dict[str, object]:
    return {
        "type": "FeatureCollection",
        "source": SOURCE,
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [site["lon"], site["lat"]]},
                "properties": {
                    key: value for key, value in site.items() if key not in {"lat", "lon"}
                },
            }
            for site in radar_sites_json(conus=conus)
        ],
    }


def find_radar_site(site_id: str) -> dict[str, object] | None:
    site = _RADAR_SITES_BY_ID.get(site_id.upper())
    return None if site is None else site.to_json()


__all__ = [
    "LOWER_48_STATES",
    "RADAR_SITES",
    "SOURCE",
    "RadarSite",
    "all_radar_sites",
    "conus_radar_sites",
    "find_radar_site",
    "radar_sites_geojson",
    "radar_sites_json",
]
