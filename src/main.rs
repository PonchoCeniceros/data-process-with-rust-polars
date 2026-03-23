// Importa las herramientas para el Lazy API.
// El modo Lazy construye un plan de consulta
// y solo lo ejecuta cuando llamas a .collect()
use polars::lazy::prelude::*;
// Importa los tipos básicos necesarios, como Series,
// ChunkedArray y funciones de agregación.
use polars::prelude::*;

// simpliciación de los tipos de dato para la aplicación
pub type DataFrame = polars::frame::DataFrame;
pub type PolarsResult<T> = Result<T, polars::error::PolarsError>;

/**
 * Función para generar un DF a partir de un archivo .csv
 */
pub fn load_csv(path: &str) -> PolarsResult<DataFrame> {
    let file = std::fs::File::open(path).unwrap();
    CsvReader::new(file).finish()
}

/**
 * Función principal
 */
fn main() {
    println!("Hello, world!");

    let df = load_csv("src/data/data.csv").unwrap();
    let lf = df.clone().lazy();

    let data: PolarsResult<DataFrame> = lf
        .select([col("Extension"), col("Queue")])
        .sort(["Queue"], Default::default())
        .collect();

    let ans = data.unwrap();
    println!("\n=== Polars ===");
    println!("{}", ans);
}
