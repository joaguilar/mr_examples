from mrjob.job import MRJob
from mrjob.step import MRStep

class Clasificaciones(MRJob):
    """
    Clase Clasificaciones: job de MapReduce que calcula cuántas reseñas (calificaciones)
    tiene cada película y luego ordena las películas por la cantidad de reseñas en orden descendente.

    Este script tiene dos pasos ("steps"):
    Paso 1: Contar la cantidad de reseñas por película.
            - Mapper: emite (movieID, 1) para cada reseña encontrada.
            - Reducer: suma todos los 1's para cada movieID y emite con una clave fija para el siguiente paso.
                    
    Paso 2: Ordena la lista de películas por la cantidad de reseñas 
        - Reducer: recibe todos los pares clave/valor y las ordena.

    El paso 2 **no funciona bien para datasets extremadamente grandes**, 
    porque todo el conjunto de datos se carga en memoria.
    Para conjuntos pequeños o medianos, funciona sin problemas.

    Uso:
    - Desde la línea de comandos:
        python 04_MasResenas.py ratings_random_noheader.csv
    - ratings_random_noheader.csv contiene los datos de la tabla ratings sin los headings.
    """    
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_resenas,
                   reducer=self.reducer_count_resenas),
            MRStep(reducer=self.reducer_ordenar_resenas)
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
    def reducer_count_resenas(self, key, values):
        """
        Suma las reseñas por película y las empaqueta con una clave común para el siguiente paso.

        1. Suma todos los 1's para calcular cuántas reseñas tiene la película.
        2. Emite con la clave fija 
           'PASO LAS PELICULAS COMO EL VALOR, TODAS CON LA MISMA LLAVE PARA QUE SEAN PROCESADAS POR EL MISMO REDUCER'
           para que todos los resultados lleguen al mismo reducer en el siguiente paso.
        3. El valor emitido es una tupla: (cantidad de reseñas, movieID).
        """
        yield 'PASO LAS PELICULAS COMO EL VALOR, TODAS CON LA MISMA LLAVE PARA QUE SEAN PROCESADAS POR EL MISMO REDUCER', \
            (sum(values), key)
                
    def reducer_ordenar_resenas(self, _, values):
        """
        Reducer del segundo paso: ordena las películas por la cantidad de reseñas


        Ejemplo:
        clave: 'PASO LAS PELICULAS COMO EL VALOR, TODAS CON LA MISMA LLAVE PARA QUE SEAN PROCESADAS POR EL MISMO REDUCER' - descartada
        values: [(3, "31"), (10, "5"), (7, "42")]
        Salida:
            ("5", 10)
            ("42", 7)
            ("31", 3)
        """
        for value,key in sorted(values,reverse=True):
            yield key, value

if __name__ == '__main__':
    Clasificaciones.run()
