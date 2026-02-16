use "Master_magueros"

//1. Analizar con find la colección. Se muestra una salida de
    db.movies.find();
    db.movies.find({"year":1900});
    db.movies.find({and: [{ year: 1901 }, { cast: { $not: { $size: 0 } } }  ]});
    db.movies.find({cast: { $not: { $size: 0 }}}); // no vacio;

// 2. numero de documentos
    db.movies.countDocuments()

//3. Insertar una película.
        db.movies.insertOne( {  "title": "MongoDB Film",
                            "year": 2025, 
                            "cast": ["Marta Agüeros","Pedro Pedrito", "Amenabar"],
                            "genre": "Ficción" 
    })

    db.movies.find({ title: "MongoDB Film" })


//4. Borrar la pelicula insertada anteriormente
    db.movies.deleteOne({ title: "MongoDB Film" })


/*
5. Contar cuantas películas tienen actores (cast) que se llaman “and”.stos nombres de actores están por ERROR. Se
muestra una salida de ejemplo, el resultado es erróneo.
*/
    var actores_and = { cast: "and" }
    db.movies.find(actores_and).count()// salida 93

/*
6. Actualizar los documentos cuyo actor (cast) tenga por error el valor “and” como si realmente fuera un actor. Para
ello, se debe sacar únicamente ese valor del array cast. Por lo tanto, no se debe eliminar ni el documento (película) ni
su array cast con el resto de actores. Se muestra una salida de ejemplo, el resultado es erróneo.
*/
    var actores_and = { cast: "and" }
    db.movies.find(actores_and)  // visualizamos los actores que cumplen esta condicion
    
    var a = {}
    var b = { $pull: { cast: "and" } }  // la funcion pull elimina del array todos los elementos que coinciden con 'and' 
    db.movies.updateMany(a, b)  // funcion para actualizar
    
    // comprobamos que ahora sea 0
    var actores_and = { cast: "and" }
    db.movies.find(actores_and).count()

/*7. Contar cuantos documentos (películas) tienen el array ‘cast’ vacío. Se muestra una salida de ejemplo, el resultado es
erróneo.
*/

   // podemos asegurarnos que tenga el array cast ademas de que este vacio 
    var existe_vacio= {  cast: { $exists: true, $type: "array", $size: 0 } }
    db.movies.find(existe_vacio).count(); //986
    
    // comprobamos si en algun caso el array cast no existe
    db.movies.find({ cast: { $exists: false } })
    
    // por lo tanto podemos hace directamente
    var vacio = { cast: { $size: 0 } };
    db.movies.find(vacio).count()

/*
8.Actualizar TODOS los documentos (películas) que tengan el array cast vacío, añadiendo un nuevo elemento dentro
del array con valor Undefined. Cuidado! El tipo de cast debe seguir siendo un array. El array debe ser así -> [
"Undefined" ]. Se muestra una salida de ejemplo, el resultado es erróneo.
*/
    /* la funcion push añade un valor, en nuestro caso como solo queremos actualizar 
     los vacios nos vale, hemos de tener ojo de poner bien los filtros porque sino añadiriamos Undefined a todos */
    var vacio = { cast: { $size: 0 } }
    var modificacion = { $push: { cast: "Undefined" } };
    db.movies.updateMany(vacio, modificacion)
    // deberia dar 986 como antes
    var Peliculas_modificadas =db.movies.find({ cast: "Undefined" }).count();
    print("las peliculas modificadas son",Peliculas_modificadas); 
    
    // validaciones extras 
    db.movies.find({ cast: "Undefined" });
    db.movies.find({ cast: { $ne: "Undefined" } });

/*9. Contar cuantos documentos (películas) tienen el array genres vacío. Se muestra una salida de ejemplo, el resultado
es erróneo */

    var genres = { genres: { $size: 0 } }
    var conteo =db.movies.find(genres).count()
    print("Encontramos",conteo, "documentos con el array genres vacío"); 

/*10.Actualizar TODOS los documentos (películas) que tengan el array genres vacío, añadiendo un nuevo elemento
dentro del array con valor Undefined. Cuidado! El tipo de genres debe seguir siendo un array. El array debe ser así -> [
"Undefined" ]. Se muestra una salida de ejemplo, el resultado es erróneo.*/

    var genres_vacios = { genres: { $size: 0 } }
    var Undefined = { $push: { genres: "Undefined" } }
    db.movies.updateMany(genres_vacios, Undefined)
    var salida_modificada = db.movies.find({ genres: "Undefined" }).count()
    print("Se han modificado",salida_modificada, "registros")

   // validaciones extras 

    db.movies.countDocuments({ genres: "Undefined" });
    db.movies.find({ genres: { $ne: "Undefined" } });
    
/*11.Mostrar el año más reciente / actual que tenemos sobre todas las películas. Se muestra una salida de ejemplo, el
resultado es erróneo.*/

    var docs_recientes= {}
    var año = { "_id": 0, "year": 1 }
    db.movies.find(docs_recientes, año).sort({ "year": -1 }).limit(1)


/*
12.Contar cuántas películas han salido en los últimos 20 años. Debe hacerse desde el último año que se tienen
registradas películas en la colección, no desde la fecha actual, mostrando el resultado total de esos años, no el
resultado por año. Revisar que no se cuentan 21 años. Se debe hacer con el Framework de Agregación. Se muestra una
salida de ejemplo, el resultado es erróneo.
*/
    // primero buscamos cual es el ultimos año para poder calcular los 20 años
    var docs_recientes = {};
    var año_partida_final  = { "_id": 0, "year": 1 };
    var mas_reciente = db.movies.find(docs_recientes, año_partida_final ).sort({ "year": -1 }).limit(1);
    var ultimo_año =mas_reciente[0].year
    print("El ultimo año registrado es:", ultimo_año);
    //ultimos 20 en base al ultimo
    var Ultimos20 =  [{$match: {year: { $gte: ultimo_año - 19, $lte: ultimo_año }  }}
                        , {
                        $count:"Total_peliculas"
                        }];
    var resultado = db.movies.aggregate(Ultimos20).toArray()[0];;
    
    
    var años_max_min = db.movies.aggregate([{
                                            $match: { year: { $gte: ultimo_año - 19, $lte: ultimo_año } }},
                                            { 
                                            $group: { _id: null, year_min: { $min: "$year" }, 
                                                                 year_max: { $max: "$year" } } 
                                                
                                            }]).toArray()[0];
    printjson({
              Total_peliculas: resultado.Total_peliculas,
              year_min: años_max_min.year_min,
              year_max: años_max_min.year_max
              });
    
    print("En los ultimos 20 años se encuentran", resultado.Total_peliculas,".","Los años contemplados son entre",años_max_min.year_min,"y",años_max_min.year_max )

/*
13. Contar cuántas películas han salido en la década de los 60 (del 60 al 69 incluidos). Se debe hacer con el Framework
de Agregación. Se muestra una salida de ejemplo, el resultado es erróneo.
*/

    var decada_60 = { "year": { $gte: 1960, $lte: 1969 } }
    var conteo = [{ $match: decada_60 }, { $count: "total_decada60" }]
    var total_decada= db.movies.aggregate(conteo).toArray()[0]
    print("En la decada de los 60 encontramos un total de",total_decada.total_decada60,"peliculas registradas")

/*
14. Mostrar el año u años con más películas mostrando el número de películas de ese año. 
Revisar si varios años pueden compartir tener el mayor número de películas. 
Se muestra una salida de ejemplo, el resultado es erróneo.
*/
        var agrupacion = db.movies.aggregate([
          { $group: { _id: "$year", total: { $sum: 1 } } },
          { $sort: { total: -1 } }  // ordenar descendente
        ]).toArray();
        
        //  el número máximo de películas
        var max_total = agrupacion[0].total;
        var años_max = agrupacion.filter(doc => doc.total === max_total);
        
        
        // imprimir en texto normal
        print(
          "El máximo de películas es " + max_total +
          " y se cumple en los siguientes años: " + años_max.map(doc => doc._id).join(", ")
        );
        
        printjson({
          "Total peliculas": max_total,
          "año/años": años_max.map(doc => doc._id)
        });

/*
15.Mostrar el año u años con menos películas mostrando el número de películas de ese año. Revisar si varios años
pueden compartir tener el menor número de películas. Se muestra una salida de ejemplo, el resultado es erróneo. 
*/
        var agrupacion = db.movies.aggregate([
          { $group: { _id: "$year", total: { $sum: 1 } } },
          { $sort: { total: 1 } }  // orden asc
        ]).toArray();
        
        //  el número min de películas
        var min_total = agrupacion[0].total;
        var años_min = agrupacion.filter(doc => doc.total === min_total);
       
       
        print(
          "El mínimo de películas es " + min_total +
          " y se cumple en los siguientes años: " + años_min.map(doc => doc._id).join(", ")
        );
        printjson({
          "Total peliculas": min_total,
          "año/años": años_min.map(doc => doc._id)
        });



/*
16. Guardar en nueva colección llamada “actors” realizando la fase $unwind por cast, no se debe agrupar de nuevo y
debe contener todas las claves (title,year,cast,genres). Después, contar cuantos documentos existen en la nueva
colección. Se muestra una salida de ejemplo, el resultado es erróneo.
*/

    db.movies.aggregate([
      { $unwind: "$cast" },
      { $project: { _id: 0, title: 1, year: 1, cast: 1, genres: 1 } },
      { $out: "actors" }
    ]);
    
    db.actors.countDocuments()

/*
17.Sobre actors (nueva colección), mostrar la lista con los 5 actores que han participado en más películas mostrando el
número de películas en las que ha participado. Importante! Se necesita previamente filtrar para descartar aquellos
actores llamados "Undefined". Aclarar que no se eliminan de la colección, sólo que filtramos para que no aparezcan. Se
muestra una salida de ejemplo, el resultado es erróneo.
*/

    db.actors.aggregate([
      { $match: { cast: { $ne: "Undefined" } } },
      { $group: { _id: "$cast", total_peliculas: { $sum: 1 } } },
      { $sort: { total_peliculas: -1 } },
      { $limit: 5}
    ]).toArray();


/*
18.Sobre actors (nueva colección), agrupar por película y año mostrando las 5 en las que más actores hayan
participado, mostrando el número total de actores. Se muestra una salida de ejemplo, el resultado es erróneo.
*/

db.actors.aggregate([
    { $group: { _id: { title: "$title", year: "$year" }, total_actores: { $sum: 1 } } },
    { $sort: { total_actores: -1 } },
    { $limit: 5 }
    ]).toArray();

/*
19.Sobre actors (nueva colección), mostrar los 5 actores cuya carrera haya sido la más larga. Para ello, se debe
mostrar cuándo comenzó su carrera, cuándo finalizó y cuántos años ha trabajado. Importante! Se necesita previamente
filtrar para descartar aquellos actores llamados "Undefined". Aclarar que no se eliminan de la colección, sólo que
filtramos para que no aparezcan. Se muestra una salida de ejemplo, el resultado es erróneo
*/

    db.actors.aggregate([
         { $match: { cast: { $ne: "Undefined" } } },
         { $group: {_id: "$cast", inicio_carrera: { $min: "$year" }, fin_carrera: { $max: "$year" }}}, 
         { $project: {actor: "$_id", inicio_carrera: 1, fin_carrera: 1, total_carrera: { $add: [ { $subtract: ["$fin_carrera", "$inicio_carrera"] }, 1 ] }, _id: 0}},
         { $sort: { total_carrera: -1 }},
         { $limit: 5 }
        ]).toArray();
        


/*
20. Sobre actors (nueva colección), Guardar en nueva colección llamada “genres” realizando la fase $unwind por
genres, no se debe agrupar de nuevo y debe contener todas las claves (title,year,cast,genres). Después, contar cuantos
documentos existen en la nueva colección. Se muestra una salida de ejemplo, el resultado es erróneo.
*/
        
        db.actors.aggregate([
                { $unwind: "$genres" },
                { $project: { _id: 0, title: 1, year: 1, cast: 1, genres: 1 } },
                { $out: "genres" }
            ]);
        
        db.genres.countDocuments()


/*
21. Sobre genres (nueva colección), mostrar los 5 documentos agrupados por “Año y Género” que más número de
películas diferentes tienen mostrando el número total de películas. Importante! Se necesita previamente filtrar para
descartar aquellos genres llamados "Undefined". Aclarar que no se eliminan de la colección, sólo que filtramos para
que no aparezcan. Se muestra una salida de ejemplo, el resultado es erróneo.
*/
    // necesitamos la funcion addtoset para coger las peliculas diferentes
    db.genres.aggregate([
         { $match: { genres: { $ne: "Undefined" } } },
         { $group:{ _id:{ year: "$year", genre: "$genres" }, peliculas: { $addToSet: "$title" } }},
         { $project: { year: "$_id.year",genre: "$_id.genre",peliculas: { $size: "$peliculas" }}},
         { $sort: { peliculas: -1 } },
        ]).limit(5);
    

/*
22.Sobre genres (nueva colección), mostrar los 5 actores y los géneros en los que han participado con más número de
géneros diferentes, se debe mostrar tanto el número de géneros diferentes que ha interpretado como los diferentes
géneros además del nombre del actor. Importante! Se necesita previamente filtrar para descartar aquellos actores
llamados "Undefined". Aclarar que no se eliminan de la colección, sólo que filtramos para que no aparezcan. Se
muestra una salida de ejemplo, el resultado es erróneo.
*/
 
   db.genres.aggregate([
         { $match: { cast: { $ne: "Undefined" } } },
         { $group:{ _id:{ cast: "$cast" }, genres: { $addToSet: "$genres" } }},
         { $project: { actor: "$_id.cast",NumGenres: { $size: "$genres" },genres:1}},
         { $sort: { NumGenres: -1 } },
        ]).limit(5);


/*
23. Sobre genres (nueva colección), mostrar las 5 películas y su año correspondiente en los que más géneros diferentes
han sido catalogados, mostrando esos géneros y el número de géneros que contiene. Importante! Se necesita
previamente filtrar para descartar aquellos genres llamados "Undefined". Aclarar que no se eliminan de la colección,
sólo que filtramos para que no aparezcan. Se muestra una salida de ejemplo, el resultado es erróneo.
*/

   db.genres.aggregate([
         { $match: { genres: { $ne: "Undefined" } } },
         { $unwind: "$genres" },
         { $group:{ _id:{ year: "$year", peliculas: "$title" }, genres: { $addToSet: "$genres" } }},
         { $project: {peliculas: "$_id.title" ,year: "$_id.year",NumGenres: { $size: "$genres" },genres:1}},
         { $sort: { NumGenres: -1 } },
        ]).limit(5);

/*
24. Peliculas con un solo actor
*/

db.movies.aggregate([
{$project:{title:1,year:1,num_actores:{$size:"$cast"}}},
{$match:{num_actores:1}},
{$sort:{year:1}}
]);

/*
25.Mostrar la media de actores por generos ordenado por media de actores de mayor a menor,
*/

db.genres.aggregate([
{$match:{cast:{$ne:"Undefined"},genres:{$ne:"Undefined"}}},
{$group:{_id:{title:"$title",genres:"$genres"},num_actores:{$sum:1}}}, 
{$group:{_id:"$_id.genres",media_actores:{$avg:"$num_actores"}}}, 
{$sort:{media_actores:-1}}
]);


/*
26.Mostrar los 5 actores que han trabajado únicamente en películas de una sola década. Mostrar actor, década y número de películas
*/

db.actors.aggregate([
{$match:{cast:{$ne:"Undefined"}}},// eliminamos los undefined
{$project:{cast:1,decade:{$multiply:[{$floor:{$divide:["$year",10]}},10]}}}, //calculamos las decadas para poder hacer el ejercicio
{$group:{_id:{actor:"$cast",decade:"$decade"},num_peliculas:{$sum:1}}}, // aqui contamos peliculas por decada y actor
{$group:{_id:"$_id.actor",decadas:{$addToSet:"$_id.decade"},total_peliculas:{$sum:"$num_peliculas"}}},// agrupa por actor y guarda ls distintas decdas con el sumatoria total de pleiculas
{$match:{decadas:{$size:1}}}, // coge los que solo tienen 1
{$sort:{total_peliculas:-1}},
{$project:{actor:"$_id",decada:{$arrayElemAt:["$decadas",0]},total_peliculas:1,_id:0}}

]);





