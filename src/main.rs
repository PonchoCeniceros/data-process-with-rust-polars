use colored::*;
use std::error::Error;

// Importa las herramientas para el Lazy API.
// El modo Lazy construye un plan de consulta
// y solo lo ejecuta cuando llamas a .collect()
use polars::lazy::prelude::*;
// Importa los tipos básicos necesarios, como Series,
// ChunkedArray y funciones de agregación.
use polars::prelude::*;

// motor de SQL integrado que te permite registrar
// DataFrames o LazyFrames como tablas temporales
// y lanzarles consultas SQL directamente.
use polars_sql::SQLContext;

// simpliciación de los tipos de dato para la aplicación
pub type StdFile = std::fs::File;
pub type DataFrame = polars::frame::DataFrame;
pub type LazyFrame = polars::lazy::frame::LazyFrame;
pub type PolarsResult<T> = Result<T, polars::error::PolarsError>;

/**
 * Función para generar un DF a partir de un archivo .csv
 */
pub fn load_csv(path: &str) -> PolarsResult<DataFrame> {
    let file = StdFile::open(path)?;
    CsvReader::new(file).finish()
}

fn polars_query(lf: LazyFrame) -> Result<(), Box<dyn Error>> {
    let resl: PolarsResult<DataFrame> = lf
        .select([col("Transaction ID"), col("Total Amount")])
        .sort(["Total Amount"], Default::default())
        .collect();
    let data = resl?;
    println!("{}", data);
    Ok(())
}

// Transaction ID,Date,Customer ID,Gender,Age,Product Category,Quantity,Price per Unit,Total Amount

fn sql_query(lf: LazyFrame) -> Result<(), Box<dyn Error>> {
    let mut ctx = SQLContext::new();
    ctx.register("sales", lf);
    let data = ctx.execute(
        r#"
        SELECT "Product Category", sum("Total Amount") AS "Total per Category"
        FROM sales
        GROUP BY "Product Category"
        ORDER BY "Total per Category" ASC
        "#,
    )?;
    let ans = data.collect()?;
    println!("{}", ans);
    Ok(())
}

/**
 * Función principal
 */
fn main() -> Result<(), Box<dyn Error>> {
    // extrayebndo la informacion (en este caso de un dataset pero puede ser de diversas fuentes,
    // sqlite, db, etc.) y generando un DataFrame, mas concretamente un LazyFrame
    let file_path = "src/data/retail_sales_dataset.csv";
    let df = load_csv(file_path)?;
    let lf = df.lazy();

    print!(
        "{}",
        "Transacciones ordeneas por cantidad total\n"
            .black()
            .bold()
            .italic()
            .on_bright_blue()
    );
    polars_query(lf.clone())?;
    print!(
        "{}",
        "SQL: Agrupar totales por producto\n"
            .black()
            .bold()
            .italic()
            .on_bright_yellow()
    );
    sql_query(lf.clone())?;

    Ok(())
}
