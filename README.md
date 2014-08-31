MapReduceJoins
==============

CS6301 - Big data - Assignment II In-memory and Reduce side joins

###Top10ActionMovies
---

To find top 10 average rated "Action" movies with descending order of rating.
This is in-memory map side join. The smallest file is put in the memory. If any movie has multiple genres and if one of its genre is Action then it is considered to be an Action movie.


The above question can be represented as a simple SQL query as shown below

```MySQL
Select MovieID,  MovieName,  MovieGenre,  AVG(rating)
from ratings, movies
where
    movies.MovieID = ratings.MovieID and
    movies. MovieGenre = "Action"
group by (movies.MovieID, movies.MovieName,  movies.MovieGenre)
order by desc
limit 10 
```

Result

```
2905    Sanjuro (1962)  Action|Adventure        4.609
2019    Seven Samurai (The Magnificent Seven) (Shichinin no samurai) (1954)       Action|Drama    4.561
858     Godfather, The (1972)   Action|Crime|Drama      4.525
1198    Raiders of the Lost Ark (1981)  Action|Adventure        4.478
260     Star Wars: Episode IV - A New Hope (1977)       Action|Adventure|Fantasy |Sci-Fi   4.454
1221    Godfather: Part II, The (1974)  Action|Crime|Drama      4.358
2028    Saving Private Ryan (1998)      Action|Drama|War        4.337
2571    Matrix, The (1999)      Action|Sci-Fi|Thriller  4.316
1197    Princess Bride, The (1987)      Action|Adventure|Comedy|Romance 4.304
```

###ReduceSideJoin
---

To list all the movies whose genre is Action or Drama and the average movie rating is in between 4.4 - 4.7 rated by male user

The above question can be represented as a simple SQL query as shown below

```MySQL
Select MovieID,  MovieName, MovieGenre, AVG(rating)
from users, ratings, movies
where
    users.UserID = ratings.UserID  and
    movies.MovieID = ratings.MovieID and
    users.Gender = "M" and
    (movies. MovieGenre = "Action" or movies. MovieGenre  = "Drama")
group by (movies.MovieID, movies.MovieName,  movies.MovieGenre)
having  AVG(rating)  >= 4.4 and AVG(rating)  <= 4.7
```

Result

```
1178            Paths of Glory (1957)           Drama|War               4.47
1198            Raiders of the Lost Ark (1981)          Action|Adventure                4.48
1207            To Kill a Mockingbird (1962)            Drama           4.43
1795            Callejï¿½n de los milagros, El (1995)           Drama           4.5
2019            Seven Samurai (The Magnificent Seven) (Shichinin no samurai) (1954)             Action|Drama            4.56
2309            Inheritors, The (Die Siebtelbauern) (1998)              Drama           4.5
2480            Dry Cleaning (Nettoyage ï¿½ sec) (1997)         Drama           4.5
2503            Apple, The (Sib) (1998)         Drama           4.67
260             Star Wars: Episode IV - A New Hope (1977)               Action|Adventure|Fantasy|Sci-Fi         4.45
2905            Sanjuro (1962)          Action|Adventure                4.61
3030            Yojimbo (1961)          Comedy|Drama|Western            4.4
318             Shawshank Redemption, The (1994)                Drama           4.55
3517            Bells, The (1926)               Crime|Drama             4.5
3888            Skipped Parts (2000)            Drama|Romance           4.5
439             Dangerous Game (1993)           Drama           4.5
527             Schindler's List (1993)         Drama|War               4.51
557             Mamma Roma (1962)               Drama           4.5
578             Hour of the Pig, The (1993)             Drama|Mystery           4.5
668             Pather Panchali (1955)          Drama           4.4
670             World of Apu, The (Apur Sansar) (1959)          Drama           4.41
858             Godfather, The (1972)           Action|Crime|Drama              4.52
912             Casablanca (1942)               Drama|Romance|War               4.41
```
