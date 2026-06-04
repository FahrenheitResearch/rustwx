use super::*;

#[test]
fn parses_satellite_product_slugs() {
    assert_eq!(
        GoesSatelliteProduct::parse("goes_geocolor").unwrap(),
        GoesSatelliteProduct::GeoColor
    );
    assert_eq!(
        GoesSatelliteProduct::parse("goes_abi_band_13").unwrap(),
        GoesSatelliteProduct::AbiBand(13)
    );
    assert_eq!(
        GoesSatelliteProduct::parse("C02").unwrap(),
        GoesSatelliteProduct::AbiBand(2)
    );
}

#[test]
fn required_channels_are_deduped_and_sorted() {
    let products = vec![
        GoesSatelliteProduct::FireTemperatureRgb,
        GoesSatelliteProduct::AbiBand(13),
        GoesSatelliteProduct::DustRgb,
    ];
    assert_eq!(required_channels(&products), vec![5, 6, 7, 11, 13, 14, 15]);
}

#[test]
fn parses_s3_list_objects() {
    let xml = r#"
    <ListBucketResult>
      <Contents>
        <Key>ABI-L2-CMIPC/2026/118/06/OR_ABI-L2-CMIPC-M6C13_G18_s20261180646171_e20261180648556_c20261180649033.nc</Key>
        <LastModified>2026-04-28T06:49:04.000Z</LastModified>
        <Size>123</Size>
      </Contents>
    </ListBucketResult>
    "#;
    let objects = parse_s3_list_xml(xml);
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].size_bytes, 123);
    assert!(objects[0].key.ends_with(".nc"));
}

#[test]
fn bucket_parser_accepts_goes_west_aliases() {
    assert_eq!(bucket_for_satellite("G18").unwrap(), "noaa-goes18");
    assert_eq!(bucket_for_satellite("goes-18").unwrap(), "noaa-goes18");
    assert_eq!(bucket_for_satellite("noaa-goes18").unwrap(), "noaa-goes18");
}

#[test]
fn sector_aliases_resolve_to_abi_products() {
    assert_eq!(
        resolve_abi_product("ABI-L2-CMIPC", Some("full-disk")).unwrap(),
        "ABI-L2-CMIPF"
    );
    assert_eq!(
        resolve_abi_product("ABI-L2-CMIPC", Some("meso1")).unwrap(),
        "ABI-L2-CMIPM1"
    );
    assert_eq!(
        resolve_abi_product("ABI-L2-CMIPC", Some("m2")).unwrap(),
        "ABI-L2-CMIPM2"
    );
    assert_eq!(sector_slug_for_abi_product("ABI-L2-CMIPF"), "full_disk");
    assert_eq!(sector_slug_for_abi_product("ABI-L2-CMIPM1"), "mesoscale_1");
    assert_eq!(goes_s3_prefix_product("ABI-L2-CMIPM1"), "ABI-L2-CMIPM");
    assert!(abi_filename_product_matches_request(
        "ABI-L2-CMIPM1",
        "ABI-L2-CMIPM"
    ));
    assert!(abi_filename_product_matches_request(
        "ABI-L2-CMIPM1",
        "ABI-L2-CMIPM1"
    ));
    assert!(!abi_filename_product_matches_request(
        "ABI-L2-CMIPM2",
        "ABI-L2-CMIPM1"
    ));
}

#[test]
fn full_disk_defaults_avoid_high_resolution_visible_channels() {
    let products = product_inputs_for_request(&default_satellite_products(), "ABI-L2-CMIPF", false);
    let parsed = requested_products(&products).unwrap();
    assert_eq!(required_channels(&parsed), vec![13]);
}

#[test]
fn full_disk_rejects_high_resolution_visible_channels_without_opt_in() {
    let err = validate_requested_channels_for_product("ABI-L2-CMIPF", &[2, 13], false).unwrap_err();
    assert!(
        err.to_string()
            .contains("full-disk GOES channels C02 are high-resolution")
    );
    validate_requested_channels_for_product("ABI-L2-CMIPF", &[2, 13], true).unwrap();
}
