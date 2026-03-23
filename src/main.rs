use colored::*;

// Importa las herramientas para el Lazy API.
// El modo Lazy construye un plan de consulta
// y solo lo ejecuta cuando llamas a .collect()
use polars::lazy::prelude::*;
// Importa los tipos básicos necesarios, como Series,
// ChunkedArray y funciones de agregación.
use polars::prelude::*;

// simpliciación de los tipos de dato para la aplicación
pub type StdFile = std::fs::File;
pub type DataFrame = polars::frame::DataFrame;
pub type PolarsResult<T> = Result<T, polars::error::PolarsError>;

/**
 * Función para generar un DF a partir de un archivo .csv
 */
pub fn load_csv(path: &str) -> PolarsResult<DataFrame> {
    // estoy obteniendo mi archivo csv a traves de la monada Result
    let file_resl = StdFile::open(path);
    // resuelvo el contenido de mi monada
    match file_resl {
        Ok(file) => CsvReader::new(file).finish(),
        Err(_) => Ok(DataFrame::empty()),
    }
}

/**
 * Función que me regresa texto coloreado
 */
fn get_banners() -> (ColoredString, ColoredString) {
    let header = "== Hello world =="
        .black()
        .bold()
        .italic()
        .on_bright_green();
    let banner = "\n==    Polars   ==".black().bold().on_blue();
    (header, banner)
}

/**
 * Función principal
 */
fn main() {
    let (header, banner) = get_banners();
    println!("{}", header);
    let df = load_csv("src/data/data.csv").unwrap_or(DataFrame::empty());
    let lf = df.lazy();
    let data: PolarsResult<DataFrame> = lf
        .select([col("Extension"), col("Queue")])
        .sort(["Queue"], Default::default())
        .collect();
    let ans = data.unwrap_or(DataFrame::empty());
    println!("{}", banner);
    println!("{}", ans);
}
