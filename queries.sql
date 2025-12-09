-- number of unique film titles
SELECT COUNT(DISTINCT title) AS unique_titles
FROM movies;

-- Films Starring Lena Headey (Sorted by Year)
SELECT 
    m.title, 
    m.year, 
    m.rating
FROM movies m
JOIN movie_stars ms ON m.movieid = ms.movieid
JOIN persons p ON ms.personid = p.personid
WHERE p.name = 'Lena Headey'
ORDER BY m.year ASC;

-- Director Name and Total Gross of their films
SELECT 
    p.name AS director, 
    SUM(m.gross) AS total_gross
FROM persons p
JOIN movie_directors md ON p.personid = md.personid
JOIN movies m ON md.movieid = m.movieid
GROUP BY p.name
HAVING SUM(m.gross) IS NOT NULL
ORDER BY total_gross DESC;

-- Top 5 Comedy Films by Gross
SELECT 
    m.title, 
    m.year, 
    m.rating,
    m.gross
FROM movies m
JOIN movie_genres mg ON m.movieid = mg.movieid
JOIN genres g ON mg.genreid = g.genreid
WHERE TRIM(g.name) = 'Comedy'
ORDER BY m.gross DESC NULLS LAST
LIMIT 5;

-- Films directed by Martin Scorsese and starring Robert De Niro
SELECT 
    m.title, 
    m.year, 
    m.rating
FROM movies m
JOIN movie_directors md ON m.movieid = md.movieid
JOIN persons pd ON md.personid = pd.personid
JOIN movie_stars ms ON m.movieid = ms.movieid
JOIN persons ps ON ms.personid = ps.personid
WHERE pd.name = 'Martin Scorsese' 
  AND ps.name = 'Robert De Niro';


-- Mster View for Movie Details
CREATE OR REPLACE VIEW v_Movie_Details AS
SELECT 
    m.movieid,
    m.title,
    m.year,
    m.rating,
    m.gross,
    p_dir.name AS director,
    p_star.name AS star,
    g.name AS genre
FROM movies m
LEFT JOIN movie_directors md ON m.movieid = md.movieid
LEFT JOIN persons p_dir ON md.personid = p_dir.personid
LEFT JOIN movie_stars ms ON m.movieid = ms.movieid
LEFT JOIN persons p_star ON ms.personid = p_star.personid
LEFT JOIN movie_genres mg ON m.movieid = mg.movieid
LEFT JOIN genres g ON mg.genreid = g.genreid;

-- How to use the v_Movie_Details view
SELECT *
FROM v_Movie_Details 
WHERE genre = 'Comedy' 
LIMIT 5;


-- Stored Procedure for Films by Actor
CREATE OR REPLACE FUNCTION GetFilmsByActor(p_actor_name TEXT)
RETURNS TABLE (
    out_title TEXT,
    out_year TEXT,
    out_rating NUMERIC  
) 
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        m.title::TEXT,      
        m.year::TEXT,     
        m.rating::NUMERIC 
    FROM movies m
    JOIN movie_stars ms ON m.movieid = ms.movieid
    JOIN persons p ON ms.personid = p.personid
    WHERE p.name ILIKE p_actor_name
    ORDER BY m.year ASC;
END;
$$ LANGUAGE plpgsql;

-- How to use the GetFilmsByActor function
SELECT * FROM GetFilmsByActor('Lena Headey');


-- Stored Procedure for Director-Actor Collaboration

-- 2. Buat ulang fungsi dengan definisi yang benar
CREATE OR REPLACE FUNCTION GetDirectorActorCollab(dir_name TEXT, actor_name TEXT)
RETURNS TABLE (
    out_title TEXT,      
    out_year INTEGER,    -- Gunakan INTEGER agar sesuai dengan schema tabel Movies
    out_rating NUMERIC 
) 
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        m.Title::TEXT,     
        m.Year::INTEGER,     
        m.Rating::NUMERIC 
    FROM Movies m
    JOIN Movie_Directors md ON m.MovieID = md.MovieID
    JOIN Persons pd ON md.PersonID = pd.PersonID
    JOIN Movie_Stars ms ON m.MovieID = ms.MovieID
    JOIN Persons ps ON ms.PersonID = ps.PersonID
    WHERE pd.Name ILIKE dir_name 
      AND ps.Name ILIKE actor_name;
END;
$$ LANGUAGE plpgsql;