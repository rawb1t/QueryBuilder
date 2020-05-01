<?php
namespace QueryBuilder;

class DB
{
	private static $pdo;

	public const NULL = 'NULL';
    public const DEFAULT = 'DEFAULT';
	
	public static function init( string $host, string $user, string $pw, string $dbname, bool $use_std_stmt = false, int $port = 3306, string $charset = "utf8" )
	{
		self::$pdo = new \PDO("mysql:host=$host;port=$port;dbname=$dbname;charset=$charset", $user, $pw);

		if( !$use_std_stmt )
		{
			self::$pdo->setAttribute( \PDO::ATTR_STATEMENT_CLASS, array( 'QueryBuilder\Result', array( self::$pdo ) ) );
		}
	}

	public static function get():\PDO
	{
		return self::$pdo;
	}

	public static function do( string ...$expr ):int
	{
		return self::$pdo->exec( "DO " . implode( ",", $expr ) );
	}
	
	public static function select( ...$columns ):Select
	{
		$select = new Select;
		return $select->db( self::$pdo )->columns( $columns );
	}

	public static function insert( string ...$columns ):Insert
	{
		$insert = new Insert;
		return $insert->db( self::$pdo )->columns( $columns );
	}

	public static function delete( string $table, ?string $table_as = null ):Delete
	{
		$delete = new Delete( $table, $table_as );
		return $delete->db( self::$pdo );
	}

	public static function call( string $procedure ):Call
	{
		$call = new Call( $procedure );
		return $call->db( self::$pdo);
	}

	public static function update( string $table ):Update
	{
		$update = new Update( $table );
		return $update->db( self::$pdo);
	}

	public static function replace( string ...$columns ):Replace
	{
		$replace = new Replace( $columns );
		return $replace->db( self::$pdo );
	}
}

abstract class Query extends DB
{
	protected $db;

	public abstract function build():string;
	
	protected function db( \PDO &$db ):self
	{
		$this->db = $db;
		return $this;
	}

	protected function val_fix( $value )
	{
		if( is_string( $value ) )
		{
			if( in_array( strtoupper( $value ), [DB::NULL, DB::DEFAULT] ) )
			{
				return strtoupper( $value );
			}
			else
			{
				return "'$value'";
			}
		}

		return $value;
	}

	public function prepare( array $driver_options = [] )
	{
		return $this->db->prepare( $this->build(), $driver_options );
	}

	public function exec()
	{
		return $this->db->exec( $this->build() );
	}

	public function query()
	{
		return $this->db->query( $this->build() );
	}

	public function __toString():string
	{
		return $this->build();
	}
}

class Replace extends Query
{
	private $low_priority;

	private $columns = [];

	private $into;

	private $values = [];
	private $rows = [];
	private $values_select;

	private $order = [];

	public function __construct( string ...$columns )
	{
		$this->columns = $columns;
	}
	
	protected function columns( $columns ):self
	{
		$this->columns = $columns;
		return $this;
	}

	public function low_priority():self
	{
		$this->low_priority = true;
		$this->high_priority = false;
		return $this;
	}

	public function into( string $into ):self
	{
		$this->into = $into;
		return $this;
	}

	public function values( ...$values ):self
	{
		if( count( $values ) == count( $this->columns ) )
		{
			$this->values[] = $values;
			$this->values_select = null;
		}
		return $this;
	}

	public function rows( ...$rows ):self
	{
		if( count( $rows ) == count( $this->columns ) )
		{
			$this->rows[] = $rows;
			$this->values_select = null;
		}
		return $this;
	}

	public function values_select( Select $values_select ):self
	{
		$this->values = [];
		$this->rows = [];
		$this->values_select = $values_select;
		return $this;
	}

	public function order( string $order ):self
	{
		$this->order[] = $order;
		return $this;
	}

	public function orders( string ...$order ):self
	{
		$this->order = $order;
		return $this;
	}

	public function build():string
	{
		$query = "REPLACE ";
		if( $this->low_priority ) $query .= " LOW_PRIORITY ";
		$query .= " INTO $this->into ";
		$query .= " (" . implode(",", $this->columns) . ") ";
		$query .= " VALUES ";

		if( !empty( $this->values ) )
		{
			$query .= implode(",", array_map(function( $row )
			{
				$values = implode(",", array_map(function( $r )
				{
					return parent::val_fix( $r );
				}, $row));
				return " ($values) ";
			}, $this->values));
		}

		if( !empty( $this->rows ) )
		{
			$query .= implode(",", array_map(function( $row )
			{
				$values = implode(",", $row);
				return " ROW($values) ";
			}, $this->rows));

			if( !empty( $this->order ) )
			{
				$query .= " ORDER BY ";
				$query .= implode(",", $this->order);
			}
		}

		if( $this->values_select != null )
		{
			$query .= " ($this->values_select) ";
		}

		return $query;
	}
}

class Update extends Query
{
	private $table;

	private $low_priority;
	private $ignore;

	private $set = [];

	private $where = [];

	private $order = [];

	private $limit;

	public function __construct( string $table )
	{
		$this->table = $table;
	}

	public function low_priority():self
	{
		$this->low_priority = true;
		return $this;
	}

	public function ignore():self
	{
		$this->ignore = true;
		return $this;
	}

	public function set( string $col, $value ):self
	{
		$this->set[] = [$col, $value];
		return $this;
	}

	public function sets( ...$sets ):self
	{
		$this->set = $sets;
		return $this;
	}

	public function where( string $clause, ?string $connect = null ):self
	{
		$this->where[] = $clause;
		if( !empty( $connect ) )
		{
			$this->where[] = $connect;
		}
		return $this;
	}

	public function wheres( string ...$clause ):self
	{
		$this->where = $clause;
		return $this;
	}

	public function order( string $order ):self
	{
		$this->order[] = $order;
		return $this;
	}

	public function orders( string ...$order ):self
	{
		$this->order = $order;
		return $this;
	}

	public function limit( int $limit = 1 ):self
	{
		$this->limit = $limit;
		return $this;
	}

	public function build():string
	{
		$query = "UPDATE ";
		$query .= $this->table;
		$query .= " SET ";

		if( !empty( $this->set ) )
		{
			$query .= implode(",", array_map(function($set)
			{
				if( is_array( $set ) && count( $set ) == 2 )
				{
					$column = $set[0];
					$value = parent::val_fix( $set[1] );
					return "$column = $value";
				}
			}, $this->set));
		}

		if( !empty( $this->where ) )
		{
			$query .= " WHERE ";
			$query .= implode(" ", $this->where);
		}

		if( !empty( $this->order ) )
		{
			$query .= " ORDER BY ";
			$query .= implode(",", $this->order);
		}

		if( $this->limit != null )
		{
			$query .= " LIMIT $this->limit ";
		}

		return $query;
	}
}

class Call extends Query
{
	private $procedure;
	private $params = [];

	public function __construct( string $procedure )
	{
		$this->procedure = $procedure;
	}

	public function params( ...$params ):self
	{
		$this->params = $params;
		return $this;
	}

	public function build():string
	{
		$params = ( !empty( $this->params ) ) ? implode(",", array_map(function( $row )
		{
			return parent::val_fix( $row );
		}, $this->params)) : '';
		$query = "CALL $this->procedure($params)";
		return $query;
	}
}

class Delete extends Query
{
	private $table;
	private $table_as;

	private $low_priority;
	private $quick;
	private $ignore;

	private $where = [];

	private $order = [];

	private $limit;

	public function __construct( string $table, ?string $table_as = null )
	{
		$this->table = $table;
		if( $table_as != null )
		{
			$this->table_as = $table_as;
		}
	}

	public function low_priority():self
	{
		$this->low_priority = true;
		return $this;
	}

	public function quick():self
	{
		$this->quick = true;
		return $this;
	}

	public function ignore():self
	{
		$this->ignore = true;
		return $this;
	}

	public function where( string $clause, ?string $connect = null ):self
	{
		$this->where[] = $clause;
		if( !empty( $connect ) )
		{
			$this->where[] = $connect;
		}
		return $this;
	}

	public function wheres( string ...$clause ):self
	{
		$this->where = $clause;
		return $this;
	}

	public function order( string $order ):self
	{
		$this->order[] = $order;
		return $this;
	}

	public function orders( string ...$order ):self
	{
		$this->order = $order;
		return $this;
	}

	public function limit( int $limit = 1 ):self
	{
		$this->limit = $limit;
		return $this;
	}

	public function build():string
	{
		$query = "DELETE ";
		if( $this->low_priority ) $query .= " LOW_PRIORITY ";
		if( $this->quick ) $query .= " QUICK ";
		if( $this->ignore ) $query .= " IGNORE ";
		$query .= " FROM $this->table ";

		if( !empty( $this->table_as ) )
		{
			$query .= " AS $this->table_as";
		}

		if( !empty( $this->where ) )
		{
			$query .= " WHERE ";
			$query .= implode(" ", $this->where);
		}

		if( !empty( $this->order ) )
		{
			$query .= " ORDER BY ";
			$query .= implode(",", $this->order);
		}

		if( $this->limit != null )
		{
			$query .= " LIMIT $this->limit ";
		}

		return $query;
	}
}

class Insert extends Query
{
	private $low_priority;
	private $high_priority;

	private $ignore;

	private $columns = [];

	private $into;

	private $values = [];
	private $rows = [];
	private $values_select;

	private $order = [];

	private $odku = [];

	public function __construct( string ...$columns )
	{
		$this->columns = $columns;
	}
	
	protected function columns( $columns ):self
	{
		$this->columns = $columns;
		return $this;
	}

	public function low_priority():self
	{
		$this->low_priority = true;
		$this->high_priority = false;
		return $this;
	}

	public function high_priority():self
	{
		$this->low_priority = false;
		$this->high_priority = true;
		return $this;
	}

	public function ignore():self
	{
		$this->ignore = true;
		return $this;
	}

	public function into( string $into ):self
	{
		$this->into = $into;
		return $this;
	}

	public function values( ...$values ):self
	{
		if( count( $values ) == count( $this->columns ) )
		{
			$this->values[] = $values;
			$this->values_select = null;
		}
		return $this;
	}

	public function rows( ...$rows ):self
	{
		if( count( $rows ) == count( $this->columns ) )
		{
			$this->rows[] = $rows;
			$this->values_select = null;
		}
		return $this;
	}

	public function values_select( Select $values_select ):self
	{
		$this->values = [];
		$this->rows = [];
		$this->values_select = $values_select;
		return $this;
	}

	public function order( string $order ):self
	{
		$this->order[] = $order;
		return $this;
	}

	public function orders( string ...$order ):self
	{
		$this->order = $order;
		return $this;
	}

	public function on_duplicate_key_update( string ...$odku ):self
	{
		$this->odku = $odku;
		return $this;
	}

	public function build():string
	{
		$query = "INSERT ";
		if( $this->low_priority ) $query .= " LOW_PRIORITY ";
		if( $this->high_priority ) $query .= " HIGH_PRIORITY ";
		if( $this->ignore ) $query .= " IGNORE ";
		$query .= " INTO $this->into ";
		$query .= " (" . implode(",", $this->columns) . ") ";
		$query .= " VALUES ";

		if( !empty( $this->values ) )
		{
			$query .= implode(",", array_map(function( $row )
			{
				$values = implode(",", array_map(function( $r )
				{
					return parent::val_fix( $r );
				}, $row));
				return " ($values) ";
			}, $this->values));
		}

		if( !empty( $this->rows ) )
		{
			$query .= implode(",", array_map(function( $row )
			{
				$values = implode(",", $row);
				return " ROW($values) ";
			}, $this->rows));

			if( !empty( $this->order ) )
			{
				$query .= " ORDER BY ";
				$query .= implode(",", $this->order);
			}
		}

		if( $this->values_select != null )
		{
			$query .= " ($this->values_select) ";
		}

		if( !empty( $this->odku ) )
		{
			$query .= " ON DUPLICATE KEY UPDATE ";
			$query .= implode(",", $this->odku);
		}

		return $query;
	}
}

class Select extends Query
{
	private $with;

	private $distinct = false;
	private $distinct_row = false;

	private $high_priority;

	private $straight_join;

	private $columns = [];

	private $from;
	private $from_as;

	private $joins = [];

	private $where = [];

	private $group = [];
	private $group_rollup;

	private $having = [];

	private $order = [];
	private $order_rollup;

	private $limit;
	private $offset;

	private $last_command;
	private $last_join;

	public function __construct( ...$columns )
	{
		$this->columns = $columns;
	}
	
	protected function columns( $columns ):self
	{
		$this->columns = $columns;
		return $this;
	}

	public function with( With $with ):self
	{
		$this->with = $with;
	}

	public function distinct():self
	{
		$this->distinct = true;
		$this->distinct_row = false;
		return $this;
	}

	public function distinct_row():self
	{
		$this->distinct = false;
		$this->distinct_row = true;
		return $this;
	}

	public function high_priority():self
	{
		$this->high_priority = true;
		return $this;
	}

	public function straight_join():self
	{
		$this->straight_join = true;
		return $this;
	}

	public function from( string $from, ?string $alias = null ):self
	{
		$this->from = $from;
		$this->from_as = $alias;
		$this->last_command = 'from';
		return $this;
	}

	public function from_select( Select $select, ?string $alias = null ):self
	{
		$this->from = " ($select) ";
		$this->from_as = $alias;
		$this->last_command = 'from';
		return $this;
	}

	public function join( string $table, ?string $alias = null ):self
	{
		$join = new Join();

		$join->table( $table );

		if( !empty( $alias ) )
		{
			$join->as( $alias );
		}

		$this->last_command = 'join';
		$this->last_join = $table;

		$this->joins[$table] = $join;
		return $this;
	}

	public function join_select( string $table, Select $select, ?string $alias = null ):self
	{
		$join = new Join();

		$join->table_select( $select );

		if( !empty( $alias ) )
		{
			$join->as( $alias );
		}

		$this->last_command = 'join';
		$this->last_join = $table;

		$this->joins[$table] = $join;
		return $this;
	}

	public function inner_join( string $table, ?string $alias = null ):self
	{
		$join = new Join('inner');

		$join->table( $table );

		if( !empty( $alias ) )
		{
			$join->as( $alias );
		}

		$this->last_command = 'join';
		$this->last_join = $table;

		$this->joins[$table] = $join;
		return $this;
	}

	public function inner_join_select( string $table, Select $select, ?string $alias = null ):self
	{
		$join = new Join('inner');

		$join->table_select( $select );

		if( !empty( $alias ) )
		{
			$join->as( $alias );
		}

		$this->last_command = 'join';
		$this->last_join = $table;

		$this->joins[$table] = $join;
		return $this;
	}

	public function left_join( string $table, ?string $alias = null ):self
	{
		$join = new Join('left');

		$join->table( $table );

		if( !empty( $alias ) )
		{
			$join->as( $alias );
		}

		$this->last_command = 'join';
		$this->last_join = $table;

		$this->joins[$table] = $join;
		return $this;
	}

	public function left_join_select( string $table, Select $select, ?string $alias = null ):self
	{
		$join = new Join('left');

		$join->table_select( $select );

		if( !empty( $alias ) )
		{
			$join->as( $alias );
		}

		$this->last_command = 'join';
		$this->last_join = $table;

		$this->joins[$table] = $join;
		return $this;
	}

	public function right_join( string $table, ?string $alias = null ):self
	{
		$join = new Join('right');

		$join->table( $table );

		if( !empty( $alias ) )
		{
			$join->as( $alias );
		}

		$this->last_command = 'join';
		$this->last_join = $table;

		$this->joins[$table] = $join;
		return $this;
	}

	public function right_join_select( string $table, Select $select, ?string $alias = null ):self
	{
		$join = new Join('right');

		$join->table_select( $select );

		if( !empty( $alias ) )
		{
			$join->as( $alias );
		}

		$this->last_command = 'join';
		$this->last_join = $table;

		$this->joins[$table] = $join;
		return $this;
	}

	public function full_join( string $table, ?string $alias = null ):self
	{
		$join = new Join('full');

		$join->table( $table );

		if( !empty( $alias ) )
		{
			$join->as( $alias );
		}

		$this->last_command = 'join';
		$this->last_join = $table;

		$this->joins[$table] = $join;
		return $this;
	}

	public function full_join_select( string $table, Select $select, ?string $alias = null ):self
	{
		$join = new Join('full');

		$join->table_select( $select );

		if( !empty( $alias ) )
		{
			$join->as( $alias );
		}

		$this->last_command = 'join';
		$this->last_join = $table;

		$this->joins[$table] = $join;
		return $this;
	}

	public function using( string ...$using ):self
	{
		if( $this->last_command == 'join' )
		{
			call_user_func_array( array( $this->joins[$this->last_join], 'using' ), $using );
		}
		return $this;
	}

	public function on( string $clause, ?string $connect = null ):self
	{
		if( $this->last_command == 'join' )
		{
			$this->joins[$this->last_join]->on( $clause, $connect );
		}
		return $this;
	}

	public function ons( string ...$clause ):self
	{
		if( $this->last_command == 'join' )
		{
			call_user_func_array( array( $this->joins[$this->last_join], 'ons' ), $clause );
		}
		return $this;
	}

	public function where( string $clause, ?string $connect = null ):self
	{
		$this->where[] = $clause;
		$this->last_command = 'where';
		if( !empty( $connect ) )
		{
			$this->where[] = $connect;
		}
		return $this;
	}

	public function wheres( string ...$clause ):self
	{
		$this->where = $clause;
		$this->last_command = 'where';
		return $this;
	}

	public function group( string $group ):self
	{
		$this->group[] = $group;
		$this->last_command = 'group';
		return $this;
	}

	public function groups( string ...$group ):self
	{
		$this->group = $group;
		$this->last_command = 'group';
		return $this;
	}

	public function having( string $clause, ?string $connect = null ):self
	{
		$this->having[] = $clause;
		$this->last_command = 'having';
		if( !empty( $connect ) )
		{
			$this->having[] = $connect;
		}
		return $this;
	}

	public function havings( string ...$clause ):self
	{
		$this->having = $clause;
		$this->last_command = 'having';
		return $this;
	}

	public function order( string $order ):self
	{
		$this->order[] = $order;
		$this->last_command = 'order';
		return $this;
	}

	public function orders( string ...$order ):self
	{
		$this->order = $order;
		$this->last_command = 'order';
		return $this;
	}

	public function with_rollup():self
	{
		if( $this->last_command == 'order' )
		{
			$order_rollup = true;
		}
		elseif( $this->last_command == 'group' )
		{
			$group_rollup = true;
		}
		return $this;
	}

	public function limit( int $limit = 1 ):self
	{
		$this->limit = $limit;
		return $this;
	}

	public function offset( int $offset ):self
	{
		$this->offset = $offset;
		return $this;
	}

	public function build():string
	{
		$query = "";
		if( $this->with != null )
		{
			$query .= "$this->with ";
		}
		$query .= "SELECT ";
		if( $this->distinct ) $query .= " DISTINCT ";
		if( $this->distinct_row ) $query .= " DISTINCTROW ";
		if( $this->high_priority ) $query .= " HIGH_PRIORITY ";
		if( $this->straight_join ) $query .= " STRAIGHT_JOIN ";
		$query .= implode(", ", array_map(function( $column )
		{
			if( is_array( $column ) && count( $column ) == 2 )
			{
				return "$column[0] AS $column[1]";
			}
			else
			{
				return "$column";
			}
		}, $this->columns));
		$query .= " FROM ";
		$query .= " $this->from ";
		if( !empty( $this->from_as ) ) $query .= " AS $this->from_as ";

		if( !empty( $this->joins ) )
		{
			foreach( $this->joins as $join )
			{
				$query .= " $join ";
			}
		}

		if( !empty( $this->where ) )
		{
			$query .= " WHERE ";
			$query .= implode(" ", $this->where);
		}

		if( !empty( $this->group ) )
		{
			$query .= " GROUP BY ";
			$query .= implode(",", $this->group);
			if( $this->group_rollup )
			{
				$query .= " WITH ROLLUP ";
			}
		}

		if( !empty( $this->having ) )
		{
			$query .= " HAVING ";
			$query .= implode(" ", $this->having);
		}

		if( !empty( $this->order ) )
		{
			$query .= " ORDER BY ";
			$query .= implode(",", $this->order);
			if( $this->group_rollup )
			{
				$query .= " WITH ROLLUP ";
			}
		}

		if( $this->limit != null )
		{
			$query .= " LIMIT $this->limit ";
		}

		if( $this->offset != null )
		{
			$query .= " OFFSET $this->offset ";
		}

		return $query;
	}
}

class Join extends Query
{
	private $type;

	private $from;
	private $as;

	private $using = [];

	private $on = [];

	public function __construct( ?string $type = null )
	{
		if( in_array( strtolower( $type ), array( 'inner', 'left', 'right', 'full' ) ) )
		{
			$this->type = $type;
		}
		else
		{
			$this->type = null;
		}
	}

	public function table( string $from ):self
	{
		$this->from = $from;
		return $this;
	}

	public function table_select( Select $select ):self
	{
		$this->from = " ($select) ";
		return $this;
	}

	public function as( string $alias ):self
	{
		$this->as = $alias;
		return $this;
	}

	public function using( string ...$using ):self 
	{
		$this->using = $using;
		$this->on = [];
		return $this;
	}

	public function on( string $clause, ?string $connect = null ):self 
	{
		$this->on[] = $clause;
		$this->using = [];
		if( !empty( $connect ) )
		{
			$this->on[] = $connect;
		}
		return $this;
	}

	public function ons( string ...$clause ):self 
	{
		$this->on = $clause;
		$this->using = [];
		return $this;
	}

	public function build():string 
	{
		$query = null;
		switch( $this->type )
		{
			case 'inner': $query = " INNER JOIN "; break;
			case 'left': $query = " LEFT JOIN "; break;
			case 'right': $query = " RIGHT JOIN "; break;
			case 'full': $query = " FULL JOIN "; break;
			default: $query = " JOIN ";
		}
		$query .= " $this->from ";
		if( !empty( $this->as ) ) $query .= " AS $this->as ";
		if( !empty( $this->using ) )
		{
			$query .= " USING (" . implode(", ", $this->using) . ") ";
		}
		if( !empty( $this->on ) )
		{
			$query .= " ON ";
			$query .= implode(" ", $this->on);
		}

		return $query;
	}
}

class With extends Query
{
	private $with = [];

	private $recursive;

	public function sub_select( Select $select, string $name, array $columns = null ):self
	{
		$columns = !empty( $columns ) ? "(".implode(",", $columns).")" : '';
		$this->with[] = "$name $columns AS ($select)";
		return $this;
	}

	public function recursive():self
	{
		$this->recursive = true;
		return $this;
	}

	public function build():string
	{
		$query = "WITH ";
		if( $this->recursive ) $query .= " RECURSIVE ";
		$query .= implode(",", $this->with);

		return $query;
	}
}

class MultiQuery extends Query
{
	private $queries;

	public function __construct( Query ...$queries )
	{
		$this->queries = $queries;
	}

	public function add( Query $q )
	{
		$this->queries = $q;
	}

	public function remove( int $index ):bool
	{
		if( isset( $this->queries[$index] ) )
		{
			unset( $this->queries[$index] );
			return true;
		}
		return false;
	}

	public function build( string $sep = ";" ):string
	{
		$mq = array_map(function( $q )
		{
			return $q->build();
		}, $this->queries);

		return implode($sep, $mq);
	}
}

class Union extends Query
{
	private $selects;

	private $all;
	private $distinct;

	public function __construct( Select ...$selects )
	{
		$this->selects = $selects;
	}

	public function all()
	{
		$this->all = true;
		$this->distinct = false;
	}

	public function distinct()
	{
		$this->all = false;
		$this->distinct = true;
	}

	public function add( Select $s )
	{
		$this->selects = $s;
	}

	public function remove( int $index ):bool
	{
		if( isset( $this->selects[$index] ) )
		{
			unset( $this->selects[$index] );
			return true;
		}
		return false;
	}

	public function build():string
	{
		$mq = array_map(function( $q )
		{
			return $q->build();
		}, $this->queries);

		$sep = " UNION ";
		if( $this->all )
		{
			$sep = " UNION ALL ";
		}
		elseif( $this->distinct )
		{
			$sep = " UNION DISTINCT ";
		}

		return implode($sep, $mq);
	}
}

class Result extends \PDOStatement
{
	private $db;

	protected function __construct( $db )
	{
		$this->db = $db;
	}

	public function fetchFirstLine( int $fetch_style = \PDO::FETCH_ASSOC, int $cursor_orientation = \PDO::FETCH_ORI_NEXT, int $cursor_offset = 0 )
	{
		return $this->fetch( $fetch_style, $cursor_orientation, $cursor_offset );
	}

	public function hasResults():bool
	{
		return $this->rowCount() > 0;
	}
}