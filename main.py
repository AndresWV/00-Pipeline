# coding=utf-8
import argparse
import apache_beam as beam
from apache_beam.pvalue import PCollection
from apache_beam.options.pipeline_options import PipelineOptions
from pprint import pprint

def quitar_caracteres(palabra):
    #caracteres a reemplazar
    para_quitar = [',','.', ':','-',' ']
    #remplazar simbolos por vacio
    for simbolo in para_quitar:
        palabra = palabra.replace(simbolo, '')
    
    return palabra

def quitar_tilde(palabra):
    #hacer minuscula
    palabra = palabra.lower()
    #quitar tildes
    palabra = palabra.replace("á", "a")
    palabra = palabra.replace("é", "e")
    palabra = palabra.replace("í", "i")
    palabra = palabra.replace("ó", "o")
    palabra = palabra.replace("ú", "u")
    return palabra

def limpiar_palabras(palabra):
    palabra = quitar_caracteres(palabra)
    palabra = quitar_tilde(palabra)
    return palabra

def main():
    #leer argumentos de entradas
    parser = argparse.ArgumentParser(description="Nuestro primer pipeline")
    parser.add_argument("--entrada", help="Fichero de entrada")
    parser.add_argument("--salida", help="Fichero de salida")
    parser.add_argument("--n-palabras", type=int, help="Numero de palabras en la salida")
    
    def run_pipeline(custom_args, beam_arg):
        entrada = custom_args.entrada
        salida = custom_args.salida
        n_palabras = custom_args.n_palabras
        #Especifica donde lo va a ejecutar
        opts = PipelineOptions(beam_arg)
        
        with beam.Pipeline(options = opts) as p:
            #Para concadenar operaciones se usa |
            #leer lineas del texto de entrada
            #Formato PCOllection[str]
            lineas: PCollections[str] = p | beam.io.ReadFromText(entrada)
            #split por palabras
            palabras = lineas | beam.FlatMap(lambda a: a.split())
            #limpiar palabras
            limpiadas = palabras | beam.Map(limpiar_palabras)
            #formato 
            contadas: PCollections[Tuple[str,int]] = limpiadas | beam.combiners.Count.PerElement()
            #Ordenar las palabras por las más repetidas, donde el elemento 1 de la tupla son las apariciones
            palabras_top_lista = contadas | beam.combiners.Top.Of(n_palabras, key=lambda kv: kv[1])
            #sacar la lista
            palabras_top = palabras_top_lista | beam.FlatMap(lambda x: x)
            #para dejarlas en formato csv
            formateado = palabras_top | beam.Map(lambda kv: "%s,%d" % (kv[0], kv[1]))
            #imprimir 
            #formateado | beam.Map(pprint)

            #Escribir en fichero
            formateado | beam.io.WriteToText(salida)

    #Argumentos de entrada de apache beam
    our_args, bean_args = parser.parse_known_args()
    
    run_pipeline(our_args, bean_args)

if __name__ == '__main__':
    main()