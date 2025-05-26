from mrjob.job import MRJob
from mrjob.step import MRStep

class Clasificaciones(MRJob):
    """
    Este job de MapReduce, utilizando la biblioteca mrjob, tiene como objetivo calcular 
    la distribución (histograma) de las calificaciones (ratings) de películas.

    Estructura general de mrjob:
    - Cada job se compone de uno o varios pasos (steps).
    - Cada paso puede incluir:
        - Un mapper: función que procesa cada línea de entrada y emite pares clave-valor.
        - Un reducer: función que recibe todas las claves iguales y las reduce a un solo valor.

    Este job en particular solo tiene un paso:
    1. Mapper: para cada línea, extrae la calificación y emite (rating, 1).
    2. Reducer: suma todos los 1's para cada calificación y obtiene la cantidad total
       de veces que esa calificación aparece en el dataset.
    """
    def steps(self):
        """
        Define los pasos que componen el job.
        En este caso, un único paso compuesto por un mapper y un reducer.
        """
        return [
            MRStep(mapper=self.mapper_get_clasificaciones,
                   reducer=self.reducer_count_clasificaciones)
        ]

    def mapper_get_clasificaciones(self, _, line):
        """
        Mapper que procesa cada línea de entrada.
        
        Parámetros:
        - _: clave por defecto que mrjob ignora en este caso (usualmente la posición de la línea).
        - line: línea de texto del archivo CSV.

        Proceso:
        1. Se separa la línea por comas para obtener userID, movieID, rating y timestamp.
        2. Se emite (rating, 1) como clave-valor. Esto indica que se encontró una instancia de ese rating.

        Ejemplo:
        línea de entrada: "1,31,2.5,1260759144"
        salida: (2.5, 1)
        """
        (userID, movieID, rating, timestamp) = line.split(',')
        yield rating, 1

    def reducer_count_clasificaciones(self, key, values):
        """
        Reducer que agrega todas las ocurrencias de cada rating.

        Parámetros:
        - key: la calificación (rating).
        - values: un iterable con todos los 1's emitidos por el mapper para este rating.

        Proceso:
        1. Suma los 1's para obtener la cantidad total de veces que aparece ese rating.

        Ejemplo:
        clave: 2.5
        valores: [1, 1, 1, 1]
        salida: (2.5, 4)  # el rating 2.5 aparece 4 veces en total
        """
        yield key, sum(values)

if __name__ == '__main__':
    Clasificaciones.run()
