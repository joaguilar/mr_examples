from mrjob.job import MRJob
from mrjob.step import MRStep

class Clasificaciones(MRJob):
    """
    Job de MapReduce para contar el número de reseñas por película y
    ordenarlas de forma descendente usando claves invertidas, para
    que el ordenamiento lo realice el paso de shuffle y sort de MRJob.
    Este script tiene dos pasos ("steps"):
    
    
    Uso:
    - Desde la línea de comandos:
        python 05_MasResenasDistrib.py ratings_random_noheader.csv
    - ratings_random_noheader.csv contiene los datos de la tabla ratings sin los headings.
    
    """    
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_resenas,
                   reducer=self.reducer_count_resenas),
            MRStep(mapper=self.mapper_invertir_clave,
                   reducer=self.reducer_ordenar_resenas)
        ]

    def mapper_get_resenas(self, _, line):
        """
        Mapper del primer paso: emite (movieID, 1) para cada línea.

        Emite la clave movieID y el valor como 1, indicando una reseña para esa película.

        Ejemplo:
        Línea: "1,31,2.5,1260759144"
        Emisión: ("31", 1)
        """
        
        (userID, movieID, rating, timestamp) = line.split(',')
        yield movieID, 1

    # Produce una tupla donde el primer elemento es una cadena de marcador de posición 'PASO LAS PELICULAS COMO EL VALOR, TODAS CON LA MISMA LLAVE PARA QUE SEAN PROCESADAS POR EL MISMO REDUCER
    # y el segundo elemento es otra tupla que contiene la suma de películas vistas y el movieID
    def reducer_count_resenas(self, movieID, values):
        """
        Reducer del primer paso: suma todas las reseñas de cada película.

        Suma todos los 1's para obtener el total de reseñas para esa película.
        
        Ejemplo:
        movieID: "31"
        values: [1, 1, 1]
        Emisión: ("31", 3)
        """
        yield movieID, sum(values)

    def mapper_invertir_clave(self, movieID, total_reseñas):
        """
        Mapper del segundo paso: emite la cantidad negativa como clave para que el shuffle and sort
        lo ordene de forma ascendente (y quede del orden que lo necesitamos).

        Ejemplo:
        Entrada: ("31", 3)
        Emisión: ((-3, "31"), None)
        """
        neg_total_reseñas = -int(total_reseñas)
        yield (neg_total_reseñas, movieID), None
                
    def reducer_ordenar_resenas(self, key, _):
        """
        Reducer final: re-emite la tupla ordenada, convirtiendo la clave
        invertida a positivo nuevamente. Usa el paso de shuffle and sort para ordenar las películas.

        Entrada clave: (-total_reseñas, movieID)
        Salida: (movieID, total_reseñas)
        """
        total_reseñas_negativas, movieID = key
        yield movieID, -total_reseñas_negativas


if __name__ == '__main__':
    Clasificaciones.run()