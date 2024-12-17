use std::path::PathBuf;
use std::sync::OnceLock;
use serde_derive::Deserialize;
use tantivy::schema::Schema;
use anyhow::Result;

static DATASETS_BASE: OnceLock<PathBuf> = OnceLock::new();

#[derive(Debug, Deserialize)]
pub struct Movie {
    pub id: usize,
    pub title: String,
    pub overview: String,
    pub genres: Vec<String>,
    pub poster: String,
    pub release_date: i64,
}

pub fn set_dataset_base(base: PathBuf) {
    let _ = DATASETS_BASE.set(base);
} 

pub fn load_movies() -> Result<Vec<Movie>> {
    let path = DATASETS_BASE.get().unwrap();
    let content = std::fs::read(path.join("movies.json"))?;
    let data = serde_json::from_slice(&content)?;
    Ok(data)    
}

pub fn convert_movies_to_tantivy_doc(
    movies: Vec<Movie>,
    schema: Schema,
) -> Result<Vec<tantivy::TantivyDocument>> {
    let mut docs = Vec::new();
    
    let id_field = schema.get_field("id")?;
    let title_field = schema.get_field("title")?;
    let overview_field = schema.get_field("overview")?;
    let genres_field = schema.get_field("genres")?;
    let poster_field = schema.get_field("poster")?;
    let release_date_field = schema.get_field("release_date")?;
    
    for movie in movies {
        let mut doc = tantivy::TantivyDocument::new();
        doc.add_u64(id_field, movie.id as u64);
        doc.add_text(title_field, movie.title);
        doc.add_text(overview_field, movie.overview);
        
        for genre in movie.genres {
            doc.add_text(genres_field, genre);            
        }
        
        doc.add_text(poster_field, movie.poster);
        doc.add_i64(release_date_field, movie.release_date);
        
        docs.push(doc);
    }
    
    Ok(docs)
} 