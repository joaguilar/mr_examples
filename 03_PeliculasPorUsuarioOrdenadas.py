from mrjob.job import MRJob
from mrjob.step import MRStep

class PeliculasPorUsuario(MRJob):
    """
    Clase PeliculasPorUsuario: un job de MapReduce que calcula el número de películas 
    calificadas por cada usuario y luego lso ordena por la cantidad de películas calificadas.

    Este script tiene dos pasos ("steps"):
    Paso 1: Cuenta las películas calificadas por cada usuario.
        - Mapper: procesa el archivo y emite un (userID, 1) por cada película calificada por el usuario.
        - Reducer: suma todos los 1's para obtener el total de películas por usuario.
                       
    Paso 2: Ordena la lista de usuarios por la cantidad de películas calificadas 
        - Reducer: recibe todos los pares clave/valor y las ordena.

    El paso  2**no funciona bien para datasets extremadamente grandes**, 
    porque todo el conjunto de datos se carga en memoria.
    Para conjuntos pequeños o medianos, funciona sin problemas.

    Uso:
    - Desde la línea de comandos:
        python 03_PeliculasPorUsuarioOrdenadas.py ratings_random_noheader.csv
    - ratings_random_noheader.csv contiene los datos de la tabla ratings sin los headings.

    """

    def steps(self):
        """
        Define los dos pasos del job de MapReduce:
        1. Contar películas por usuario.
        2. Ordenar a los usuarios por la cantidad de películas calificadas
        """
        return [
            MRStep(mapper=self.mapper_usuario_pelicula,
                   reducer=self.reducer_contar_peliculas),
            MRStep(reducer=self.reducer_ordenar_peliculas)
        ]

    def mapper_usuario_pelicula(self, _, line):
        """
        Mapper: para cada línea, emite (userID, 1).
 
        Ejemplo:
        Línea: "1023,31,2.5,1260759144"
        Emisión: ("1023", 1)
        """
        (userID, movieID, rating, timestamp) = line.split(',')
        yield userID, 1

    def reducer_contar_peliculas(self, key, values):
        """
        Reducer: suma la cantidad de películas calificadas por cada usuario.

        Ejemplo:
        key: "1023"
        values: [1, 1, 1, 1]
        Emisión final: ("1023", 4)  # el usuario "1023" calificó 4 películas


        Proceso:
        1. Suma los valores (1's) para obtener la cantidad total de películas calificadas.
        2. Emite una clave constante ('CUALQUIER VALOR') y un valor que es una tupla
           (cantidad_total, userID). Este formato facilita el siguiente paso de ordenación.

        Ejemplo:
        key: "1023"
        values: [1, 1, 1, 1]
        Emisión: ('CUALQUIER VALOR', (4, "1023"))  # El usuario "1023" calificó 4 películas.
        """
        yield 'CUALQUIER VALOR', (sum(values), key)

    def reducer_ordenar_peliculas(self, _, values):
        """
        Reducer del segundo paso: ordena la lista de usuarios en orden descendente por
        la cantidad de películas calificadas y emite (userID, cantidad).

        1. Ordena las tuplas por cantidad de películas en orden descendente.
        2. Emite (userID, cantidad) para cada usuario ordenado.

        Ejemplo:
        values: [(4, "1023"), (10, "2"), (7, "3")]
        Salida:
            ("2", 10)
            ("3", 7)
            ("1023", 4)
        """
        for cantidad, userID in sorted(values, reverse=True):
            yield userID, cantidad

if __name__ == '__main__':
    # Permite que el job se ejecute como un script autónomo desde la línea de comandos.
    PeliculasPorUsuario.run()
