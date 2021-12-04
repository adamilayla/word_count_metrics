# Importation
de  pyspark  importer  SparkContext , SparkConf

#Instanciation

sparkConf  =  SparkConf (). setAppName ( " WordCounts " ). setMaster ( "local" )
sc  =  SparkContext ( conf  =  sparkConf )



if  __name__ ==  '__main__' :

    # Importation du fichier
    Fichier  =  sc . textFile ( "sample.txt" )
    
    # Lecture des mots ligne par ligne, creation des tuples (mot,1), compter le nombre d'occurence de chaque mot (mot,nombre)
    wordCounts  =  Fichier . flatMap ( lambda  line : line . split ( " " )) \
                . map ( mot lambda  : ( mot , 1 )) \
                . réduireParClé ( lambda  a , b : a + b )

    # Affichage des résultats         
    pour  i  dans  wordCounts . collecter ():
        imprimer ( je )

    # Exportation des résultats dans un fichier texte
    wordCounts . fusionner ( 1 ). saveAsTextFile ( "Resultat.txt" )
   
