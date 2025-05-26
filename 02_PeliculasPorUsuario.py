from mrjob.job import MRJob
from mrjob.step import MRStep

class PeliculasPorUsuario(MRJob):
    """
    Clase PeliculasPorUsuario: un job de MapReduce que calcula cuántas películas
    ha calificado cada usuario en el dataset de MovieLens.

    1. Mapper: procesa cada línea del archivo CSV, extrae el userID y emite (userID, 1).
    2. Reducer: suma todos los 1's asociados a cada userID para obtener el número total
    de películas calificadas por cada usuario.

    Uso:
    - Desde la línea de comandos:
        python 02_PeliculasPorUsuario.py ratings_random_noheader.csv
    - ratings_random_noheader.csv contiene los datos de la tabla ratings sin los headings.
    """

    def steps(self):
        """
        Define los pasos del job. En este caso, solo hay un paso con un mapper y un reducer.
        """
        return [
            MRStep(mapper=self.mapper_usuario_pelicula,
                   reducer=self.reducer_contar_peliculas)
        ]

    def mapper_usuario_pelicula(self, _, line):
        """
        Mapper: para cada línea, emite (userID, 1).
 
        Ejemplo:
        Línea: "1023,31,2.5,1260759144"
        Emisión: ("1023", 1)
        """
        # Dividir la línea en sus componentes
        (userID, movieID, rating, timestamp) = line.split(',')
        yield userID, 1

    def reducer_contar_peliculas(self, key, values):
        """
        Reducer: suma la cantidad de películas calificadas por cada usuario.

        Ejemplo:
        key: "1023"
        values: [1, 1, 1, 1]
        Emisión final: ("1023", 4)  # el usuario "1023" calificó 4 películas
        """
        yield key, sum(values)

if __name__ == '__main__':
    PeliculasPorUsuario.run()

