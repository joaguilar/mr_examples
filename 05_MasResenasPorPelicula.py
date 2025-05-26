from mrjob.job import MRJob
from mrjob.step import MRStep

class MasResenasPorPelicula(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_resenas,
                   reducer=self.reducer_count_resenas),
            MRStep(reducer=self.reducer_ordenar_resenas)
        ]

    # Funcion que se encarga de obtener las reseñas de las peliculas
    # y emitir un 1 por cada reseña
    def mapper_get_resenas(self, _, line):
        (userID, movieID, rating, timestamp) = line.split(',')
        yield movieID, 1

    # Funcion que se encarga de sumar las reseñas de las peliculas
    # y emitir el total de reseñas por pelicula
    def reducer_count_resenas(self, key, values):
        # yield str(sum(values)).zfill(6),key
        yield str(sum(values)).zfill(5),key

    # Funcion que se encarga de ordenar las reseñas de las peliculas
    # y emitir el total de reseñas por pelicula                
    def reducer_ordenar_resenas(self, count, movies):
        for movie in movies:
            yield movie, count
            
if __name__ == '__main__':
    MasResenasPorPelicula.run()
