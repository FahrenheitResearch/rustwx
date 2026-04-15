use rustwx_render::solar07::{
    ECAPE_SEVERE_PANEL_PRODUCTS, SEVERE_CLASSIC_PANEL_PRODUCTS, Solar07Product,
};

fn main() {
    print_panel("ecape-severe", &ECAPE_SEVERE_PANEL_PRODUCTS);
    print_panel("severe-classic", &SEVERE_CLASSIC_PANEL_PRODUCTS);
}

fn print_panel(name: &str, products: &[Solar07Product]) {
    println!("{name}:");
    for product in products {
        println!("  {} -> {}", product.slug(), product.display_title());
    }
}
