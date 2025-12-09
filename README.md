# 🎬 Movie Data ETL & Warehousing Pipeline

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14%2B-elephant)
![Status](https://img.shields.io/badge/Status-Production--Ready-green)

## 📋 Overview

This project is a scalable ETL (Extract, Transform, Load) pipeline designed to ingest raw movie metadata (`movies.csv`), clean and normalize the data, and load it into a **PostgreSQL** database. 

The pipeline transforms flat file data into a **Third Normal Form (3NF)** relational schema, ensuring data integrity and optimizing for complex analytical queries. It includes robust error handling for data type overflows and dirty data.

## 🏗 Data Architecture

We utilize a **Normalized (3NF)** approach for the operational layer to reduce redundancy. While a Star Schema is typical for pure OLAP, 3NF was chosen here to strictly enforce integrity during the raw data import phase.

### A. Conceptual Data Model
* **Entities**: `Movie`, `Person`, `Genre`.
* **Relationships**:
    * A Movie can have multiple **Genres**.
    * A Movie can have multiple **Directors** (`Persons`).
    * A Movie can have multiple **Stars** (`Persons`).
    * A **Person** can play a role in multiple movies (Many-to-Many).

### B. Logical Data Model
* **Movies**: Stores core film attributes (`Title`, `Year`, `Rating`, `Gross`, `Runtime`, `Votes`).
* **Persons**: Stores unique individuals (`Name`) to resolve the M:N relationship between movies and people.
* **Genres**: Stores unique genre names (`Name`).
* **Junction Tables**: `Movie_Directors`, `Movie_Stars`, `Movie_Genres` resolve the relationships.

### C. Physical Data Model (PostgreSQL)
* **Primary Keys**: `SERIAL` (Auto-increment) for efficient indexing.
* **Data Types**: 
    * `NUMERIC/DECIMAL`: Used for `Gross` revenue to avoid floating-point errors.
    * `BIGINT`: Used for `Votes` to handle high-volume metrics (preventing integer overflow).
    * `TEXT`: Used for strings (preferred over `VARCHAR` in Postgres for performance).
* **Indexes**: Applied on `Year` and `Rating` to optimize sorting and filtering queries.

### D. Entity Relationship Diagram (ERD)

```mermaid
erDiagram
    MOVIES ||--o{ MOVIE_STARS : features
    PERSONS ||--o{ MOVIE_STARS : acts_in
    MOVIES ||--o{ MOVIE_DIRECTORS : directed_by
    PERSONS ||--o{ MOVIE_DIRECTORS : directs
    MOVIES ||--o{ MOVIE_GENRES : classified_as
    GENRES ||--o{ MOVIE_GENRES : defines

    MOVIES {
        int MovieID
        string Title
        int Year
        decimal Rating
        bigint Votes
        decimal Gross
    }
    PERSONS {
        int PersonID
        string Name
    }
    GENRES {
        int GenreID
        string Name
    }
