import _, { isNull } from "lodash";
import { CsvLoader } from "../src/data/csv-loader";
import { Database } from "../src/database";
import {
  selectActorsByMovieId,
  selectCount,
  selectDirectorsByMovieId,
  selectGenresByMovieId,
  selectKeywordsByMovieId,
  selectMovie,
  selectMovieId,
  selectProductionCompaniesByMovieId
} from "../src/queries/select";
import {
  MOVIE_ACTORS,
  MOVIE_DIRECTORS,
  MOVIE_GENRES,
  MOVIE_KEYWORDS,
  MOVIE_PRODUCTION_COMPANIES
} from "../src/table-names";
import {
  ActorRow,
  DirectorRow,
  GenreRow,
  KeywordRow,
  ProductionCompanyRow
} from "../src/types";
import { minutes } from "./utils";

const insertMovieGenres = (
  movieId: number,
  genres: string[],//genres
  genreRows: GenreRow[]//genre in genreRows contains id and the main gener
): string => {

    const val=genres.map(genre=>{
      const genreRow=genreRows.find(row=>row.genre==genre);
      if(genreRow){
        return `(${movieId},${genreRow.id})`;
      }
      return null;
    }).filter(value=>!isNull(value)).join(',')
    
    return `INSERT INTO movie_genres (movie_id,genre_id) VALUES ${val};`;
};

const insertMovieActors = (
  movieId: number,
  actors: string[],
  actorRows: ActorRow[]
): string => {

    const val=actors.map(actor=>{
      const actorRow=actorRows.find(row=>row.full_name==actor);
      if(actorRow){
        return `(${movieId},${actorRow.id})`;
      }
      return null
    }).filter(value=>!isNull(value)).join(',')

  return `INSERT INTO movie_actors(movie_id,actor_id) VALUES ${val};`;
};

const insertMovieDirectors = (
  movieId: number,
  directors: string[],
  directorRows: DirectorRow[]
): string => {
  
  const val=directors.map(director=>{
    const directorRow=directorRows.find(row=>row.full_name==director);
    if(directorRow){
      return `(${movieId},${directorRow.id})`;
    }
    return null
  }).filter(value=>!isNull(value)).join(',')

  return `INSERT INTO movie_directors(movie_id,director_id) VALUES ${val};`;
};

const insertMovieKeywords = (
  movieId: number,
  keywords: string[],
  keywordRows: KeywordRow[]
): string => {

  const val=keywords.map(keyword=>{
    const keywordRow=keywordRows.find(row=>row.keyword==keyword);
    if(keywordRow){
      return `(${movieId},${keywordRow.id})`;
    }
    return null
  }).filter(value=>!isNull(value)).join(',')

  return `INSERT INTO movie_keywords(movie_id,keyword_id) VALUES ${val};`;
};

const insertMovieProductionCompanies = (
  movieId: number,
  productionCompanies: string[],
  productionCompanyRows: ProductionCompanyRow[]
): string => {

  const val=productionCompanies.map(productionCompany=>{
    const productionCompanyRow=productionCompanyRows.find(row=>row.company_name==productionCompany);
    if(productionCompanyRow){
      return `(${movieId},${productionCompanyRow.id})`;
    }
    return null
  }).filter(value=>!isNull(value)).join(',')

  return `INSERT INTO movie_production_companies(movie_id,company_id) VALUES ${val};`;
};

describe("Insert Relationship Data", () => {
  let db: Database;

  beforeAll(async () => {
    db = await Database.fromExisting("04", "05");
    await CsvLoader.load();
  }, minutes(3));

  it(
    "should insert genre relationship data",
    async done => {
      const movies = await CsvLoader.movies();
      const genreRows = (await db.selectMultipleRows(`SELECT * FROM genres`)) as GenreRow[];
      const moviesByImdbId = _.groupBy(await CsvLoader.movies(), "imdbId");

      for (const imdbId of Object.keys(moviesByImdbId)) {
        const movieId = (await db.selectSingleRow(selectMovieId(imdbId)))
          .id as number;
        const genres = movies.find(it => it.imdbId === imdbId)!.genres;
        if (genres.length > 0) {
          //console.log(`movie id:${movieId} genres:${genres}`);
          await db.insert(insertMovieGenres(movieId, genres, genreRows));
        }
      }

      const count = await db.selectSingleRow(selectCount(MOVIE_GENRES));
      expect(count.c).toBe(7141); 

      const movie = await db.selectSingleRow(selectMovie("tt2908446"));
      expect(movie.original_title).toBe("Insurgent");

      const genres = await db.selectMultipleRows(
        selectGenresByMovieId(movie.id as number)
      );
      expect(genres).toEqual([
        { genre: "Adventure" },
        { genre: "Science Fiction" },
        { genre: "Thriller" }
      ]);

      done();
    },
    minutes(200)
  );

  it(
    "should insert actor relationship data",
    async done => {
      const movies = await CsvLoader.movies();
      const actorRows = (await db.selectMultipleRows(`SELECT * FROM actors`)) as ActorRow[];
      const moviesByImdbId = _.groupBy(await CsvLoader.movies(), "imdbId");

      for (const imdbId of Object.keys(moviesByImdbId)) {
        const movieId = (await db.selectSingleRow(selectMovieId(imdbId)))
          .id as number;
        const actors = movies.find(it => it.imdbId === imdbId)!.cast;
        if (actors.length > 0) {
          console.log(`(${movieId},${actors}`);
          await db.insert(insertMovieActors(movieId, actors, actorRows));
        }
      }

      const count = await db.selectSingleRow(selectCount(MOVIE_ACTORS));
      expect(count.c).toBe(14306);

      const movie = await db.selectSingleRow(selectMovie("tt3659388"));
      expect(movie.original_title).toBe("The Martian");

      const actors = await db.selectMultipleRows(
        selectActorsByMovieId(movie.id as number)
      );
      expect(actors).toEqual([
        { full_name: "Matt Damon" },
        { full_name: "Jessica Chastain" },
        { full_name: "Kristen Wiig" },
        { full_name: "Jeff Daniels" },
        { full_name: "Michael PeÃ±a" }
      ]);

      done();
    },
    minutes(200)
  );

  it(
    "should insert director relationship data",
    async done => {
      const movies = await CsvLoader.movies();
      const directorRows = (await db.selectMultipleRows(`SELECT * FROM directors`)) as DirectorRow[];
      const moviesByImdbId = _.groupBy(await CsvLoader.movies(), "imdbId");

      for (const imdbId of Object.keys(moviesByImdbId)) {
        const movieId = (await db.selectSingleRow(selectMovieId(imdbId)))
          .id as number;
        const directors = movies.find(it => it.imdbId === imdbId)!.directors;
        if (directors.length > 0) {
          await db.insert(
            insertMovieDirectors(movieId, directors, directorRows)
          );
        }
      }

      const count = await db.selectSingleRow(selectCount(MOVIE_DIRECTORS));
      expect(count.c).toBe(3340);

      const movie = await db.selectSingleRow(selectMovie("tt2488496"));
      expect(movie.original_title).toBe("Star Wars: The Force Awakens");

      const directors = await db.selectMultipleRows(
        selectDirectorsByMovieId(movie.id as number)
      );
      expect(directors).toEqual([{ full_name: "J.J. Abrams" }]);

      done();
    },
    minutes(200)
  );

  it(
    "should insert keyword relationship data",
    async done => {
      const movies = await CsvLoader.movies();
      const keywordRows = (await db.selectMultipleRows(`SELECT * FROM keywords`)) as KeywordRow[];
      const moviesByImdbId = _.groupBy(await CsvLoader.movies(), "imdbId");

      for (const imdbId of Object.keys(moviesByImdbId)) {
        const movieId = (await db.selectSingleRow(selectMovieId(imdbId)))
          .id as number;
        const keywords = movies.find(it => it.imdbId === imdbId)!.keywords;
        if (keywords.length > 0) {
          await db.insert(insertMovieKeywords(movieId, keywords, keywordRows));
        }
      }

      const count = await db.selectSingleRow(selectCount(MOVIE_KEYWORDS));
      expect(count.c).toBe(9568);

      const movie = await db.selectSingleRow(selectMovie("tt2820852"));
      expect(movie.original_title).toBe("Furious 7");

      const keywords = await db.selectMultipleRows(
        selectKeywordsByMovieId(movie.id as number)
      );
      expect(keywords).toEqual([
        { keyword: "car race" },
        { keyword: "speed" },
        { keyword: "revenge" },
        { keyword: "suspense" },
        { keyword: "car" }
      ]);

      done();
    },
    minutes(200)
  );

  it(
    "should insert production companies relationship data",
    async done => {
      const movies = await CsvLoader.movies();
      const productionCompanyRows = (await db.selectMultipleRows(`SELECT * FROM production_companies`)) as ProductionCompanyRow[];
      const moviesByImdbId = _.groupBy(await CsvLoader.movies(), "imdbId");

      for (const imdbId of Object.keys(moviesByImdbId)) {
        const movieId = (await db.selectSingleRow(selectMovieId(imdbId)))
          .id as number;
        const productionCompanies = movies.find(it => it.imdbId === imdbId)!
          .productionCompanies;
        if (productionCompanies.length > 0) {
          await db.insert(
            insertMovieProductionCompanies(
              movieId,
              productionCompanies,
              productionCompanyRows
            )
          );
        }
      }

      const count = await db.selectSingleRow(
        selectCount(MOVIE_PRODUCTION_COMPANIES)
      );
      expect(count.c).toBe(7017);

      const movie = await db.selectSingleRow(selectMovie("tt0133046"));
      expect(movie.original_title).toBe("Teaching Mrs. Tingle");

      const productionCompanies = await db.selectMultipleRows(
        selectProductionCompaniesByMovieId(movie.id as number)
      );
      expect(productionCompanies).toEqual([
        { company_name: "Dimension Films" },
        { company_name: "Interscope Communications" },
        { company_name: "Konrad Pictures" }
      ]);

      done();
    },
    minutes(200)
  );
});
