# XQuery
Simple Class to build MySQL Queries. The library is using PDO to connect to databases.

Turn code like this:

```PHP
pdo = new PDO("mysql:host=localhost;port=3306;dbname=database;charset=utf-8", "root", "password");

$stmt = $pdo->prepare("SELECT t1.id, t1.value, DATE_FORMAT(t1.date, '%d.%m.%Y') AS date_formatted, t2.external FROM table1 AS t1 LEFT JOIN table2 AS t2 USING (id) WHERE name LIKE ':query' ORDER BY date DESC LIMIT 5");
$stmt->bindValue(':query', 'foobar');
$stmt->execute();
```

Into this:

```PHP
use XQuery\DB;

DB::init("localhost", "root", "password", "database");

$stmt = DB::select( "t1.id", "t1.value", ["DATE_FORMAT(t1.date, '%d.%m.%Y')", "date"], "t2.external" )
->from( "table1", "t1")
->left_join( "table2", "t2" )
->using( "id" )
->where("name LIKE ':query'")
->order( "date DESC" )
->limit(5)
->prepare();

$stmt->bindValue(':query', 'foobar');
$stmt->execute();
```
