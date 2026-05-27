"""
Framework MapReduce local para Python — reemplaza mrjob sin dependencias externas.

Implementa las tres fases del modelo MapReduce:
  1. MAP    — aplica la función mapper a cada línea de entrada
  2. SHUFFLE & SORT — agrupa y ordena los pares (clave, valor) por clave
  3. REDUCE — aplica la función reducer a cada grupo

Uso (idéntico a mrjob):
    python mi_job.py data/ratings_noheader.csv
"""

import sys
import json
from itertools import groupby


def _sort_key(k):
    """
    Genera una clave de ordenamiento comparable para cualquier tipo de dato.

    Asigna una categoría numérica a cada tipo para evitar errores al comparar
    tipos distintos (p. ej. None vs str en Python 3).

    Categorías: 0=None, 1=bool, 2=numérico, 3=str, 4=lista/tupla, 5=otro
    """
    if k is None:
        return (0,)
    if isinstance(k, bool):
        return (1, k)
    if isinstance(k, (int, float)):
        return (2, k)
    if isinstance(k, str):
        return (3, k)
    if isinstance(k, (list, tuple)):
        return (4,) + tuple(_sort_key(e) for e in k)
    return (5, str(k))


class MRStep:
    """
    Define un paso de un job MapReduce.

    Parámetros:
        mapper   — función que recibe (clave, valor) y emite pares (clave, valor)
        reducer  — función que recibe (clave, iterador_valores) y emite pares (clave, valor)

    Cualquiera de los dos puede omitirse (None).
    """

    def __init__(self, mapper=None, reducer=None):
        self.mapper = mapper
        self.reducer = reducer


class MRJob:
    """
    Clase base para jobs MapReduce locales.

    Hereda de esta clase, define los métodos mapper/reducer y declara
    los pasos en steps(). Ejecuta con:

        if __name__ == '__main__':
            MiJob.run()
    """

    def __init__(self, args=None):
        self.args = args if args is not None else sys.argv[1:]

    def steps(self):
        """Retorna la lista de pasos del job. Sobreescribir en subclases multi-step."""
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

    @classmethod
    def run(cls):
        """Punto de entrada: lee el archivo, ejecuta los steps e imprime los resultados."""
        job = cls()

        # ── Lectura de entrada ──────────────────────────────────────────────
        if job.args:
            with open(job.args[0], 'r', encoding='utf-8') as f:
                data = f.readlines()
        else:
            data = sys.stdin.readlines()

        # ── Pipeline de steps ──────────────────────────────────────────────
        is_raw = True   # Primer step recibe líneas de texto; los demás reciben (k, v)
        for step in job.steps():
            data = job._execute_step(step, data, is_raw)
            is_raw = False

        # ── Salida en formato JSON separado por tabulación ─────────────────
        for key, value in data:
            print(json.dumps(key) + '\t' + json.dumps(value))

    def _execute_step(self, step, data, is_raw):
        """Ejecuta un step completo: Map → Shuffle & Sort → Reduce."""

        # ── FASE MAP ───────────────────────────────────────────────────────
        # Si no hay mapper, se usa la función identidad para pasar los datos al sort/reduce.
        fn = step.mapper or (lambda k, v: [(k, v)])
        mapped = []
        for item in data:
            in_key, in_val = (None, item.rstrip('\n')) if is_raw else item
            for out_k, out_v in (fn(in_key, in_val) or []):
                mapped.append((out_k, out_v))

        # ── FASE SHUFFLE & SORT ────────────────────────────────────────────
        # Ordena por clave usando _sort_key, que maneja None, int, str y tuplas.
        mapped.sort(key=lambda x: _sort_key(x[0]))

        # ── FASE REDUCE ────────────────────────────────────────────────────
        if not step.reducer:
            return mapped

        result = []
        for key, group_iter in groupby(mapped, key=lambda x: x[0]):
            values = (item[1] for item in group_iter)
            for out_k, out_v in (step.reducer(key, values) or []):
                result.append((out_k, out_v))

        return result
