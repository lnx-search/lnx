struct ExampleConfig {
    example: i32,
}

#[test]
fn test_config_access() {
    let config = ExampleConfig { example: 123 };

    lnx_config::init(config).unwrap();

    let value = lnx_config::get!(ExampleConfig.example);
    println!("Got {}", value);
    assert_eq!(value, 123);
}
