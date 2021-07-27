<?php
namespace Danae\Astral;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Types\Type;


// Class that handles querying the database
class Database
{
  // Key for the fields to include in a select query
  public const SELECT_FIELDS = 'fields';

  // Key for the order in a select query
  public const SELECT_ORDER_BY = 'order_by';

  // Key for the offset in a select query
  public const SELECT_OFFSET = 'offset';

  // Key for the limit in a select query
  public const SELECT_LIMIT = 'limit';

  // Key for the filtering of distinct results in a select query
  public const SELECT_DISTINCT = 'distinct';

  // Key for the inclusion of types in array normalization
  public const NORMALIZE_TYPE_INCLUDED = 'type_included';

  // Key for type overrides in array normalization
  public const NORMALIZE_TYPE_OVERRIDES = 'type_overrides';

  // Key for the type in array normalization
  public const NORMALIZE_KEY_TYPE = 'type';

  // Key for the value in array normalization
  public const NORMALIZE_KEY_VALUE = 'value';


  // The connection of this database
  private $connection;


  // Constructor
  public function __construct(Connection $connection)
  {
    $this->connection = $connection;
  }

  // Return the connection of the database
  public function getConnection(): Connection
  {
    return $this->connection;
  }

  // Create a select query
  private function createSelect(string $table, array $where = [], array $options = []): QueryBuilder
  {
    // Get the default options
    $options[self::SELECT_FIELDS] ??= ['*'];
    $options[self::SELECT_ORDER_BY] ??= [];
    $options[self::SELECT_OFFSET] ??= null;
    $options[self::SELECT_LIMIT] ??= null;
    $options[self::SELECT_DISTINCT] ??= false;

    // Create the query
    $query = $this->connection->createQueryBuilder()
      ->select(...$options[self::SELECT_FIELDS])
      ->from($table);

    // Create the clauses
    if (!empty($where))
      self::createWhereClause($query, $where);
    if (!empty($options[self::SELECT_ORDER_BY]))
      self::createOrderByClause($query, $options[self::SELECT_ORDER_BY]);
    if ($options[self::SELECT_OFFSET] !== null)
      $query->setFirstResult($options[self::SELECT_OFFSET]);
    if ($options[self::SELECT_LIMIT] !== null)
      $query->setMaxResults($options[self::SELECT_LIMIT]);
    if ($options[self::SELECT_DISTINCT] === true)
      $query->distinct();

    // Return the query builder
    return $query;
  }

  // Execute a select query and return the results as an array
  public function select(string $table, array $where = [], array $options = []): array
  {
    // Execute the select query
    $array = $this->createSelect($table, $where, $options)->fetchAllAssociative();

    // Denormalize the results
    return array_map(fn($data) => $this->denormalizeArray($table, $data), $array);
  }

  // Execute a select query and return the first result or null
  public function selectOne(string $table, array $where = [], array $options = []): ?array
  {
    // Execute the select query
    $options = array_merge($options, [self::SELECT_LIMIT => 1]);
    $data = $this->createSelect($table, $where, $options)->fetchAssociative();

    // Denormalize the result, or return null if no result
    return $data !== false ? $this->denormalizeArray($table, $data) : null;
  }

  // Execute an insert query
  public function insert(string $table, array $data): int
  {
    // Check if the arguments are valid
    if (empty($data))
      throw new \InvalidArgumentException("data cannot be an empty array");

    // Create the query
    $query = $this->connection->createQueryBuilder()
      ->insert($table);

    // Iterate over the normalized data
    $data = $this->normalizeArray($table, $data, [self::NORMALIZE_TYPE_INCLUDED => true]);
    foreach ($data as $key => $field)
    {
      // Skip if the value is null
      if ($field[self::NORMALIZE_KEY_VALUE] === null)
        continue;

      // Set the value in the query
      $query->setValue($key, $query->createNamedParameter($field[self::NORMALIZE_KEY_VALUE]), $field[self::NORMALIZE_KEY_TYPE]);
    }

    // Execute the query
    return $query->executeStatement();
  }

  // Execute an update query
  public function update(string $table, array $data, array $where): int
  {
    // Check if the arguments are valid
    if (empty($data))
      throw new \InvalidArgumentException("data cannot be an empty array");
    if (empty($where))
      throw new \InvalidArgumentException("where cannot be an empty array");

    // Create the query
    $query = $this->connection->createQueryBuilder()
      ->update($table);
    self::createWhereClause($query, $where);

    // Iterate over the normalized data
    $data = $this->normalizeArray($table, $data, [self::NORMALIZE_TYPE_INCLUDED => true]);
    foreach ($data as $key => $field)
    {
      // Skip if the value is null
      if ($field[self::NORMALIZE_KEY_VALUE] === null)
        continue;

      // Set the value in the query
      $query->set($key, $query->createNamedParameter($field[self::NORMALIZE_KEY_VALUE]), $field[self::NORMALIZE_KEY_TYPE]);
    }

    // Execute the query
    return $query->executeStatement();
  }

  // Execute a delete query
  public function delete(string $table, array $where): int
  {
    // Check if the arguments are valid
    if (empty($where))
      throw new \InvalidArgumentException("where cannot be an empty array");

    // Create the query
    $query = $this->connection->createQueryBuilder()
      ->delete($table);
    self::createWhereClause($query, $where);

    // Execute the query
    return $query->executeStatement();
  }

  // Convert a value from its PHP representation to its database representation
  private function normalize($value, $type)
  {
    if (is_string($type))
      $type = Type::getType($type);
    return $type->convertToDatabaseValue($value, $this->connection->getDatabasePlatform());
  }

  // Convert a value from its database representation to its PHP representation
  private function denormalize($value, $type)
  {
    if (is_string($type))
      $type = Type::getType($type);
    return $type->convertToPHPValue($value, $this->connection->getDatabasePlatform());
  }

  // Convert an array of values from its PHP representation to its database representation
  private function normalizeArray(string $table, array $data, array $options = []): array
  {
    // Get the default options
    $options[self::NORMALIZE_TYPE_OVERRIDES] ??= [];
    $options[self::NORMALIZE_TYPE_INCLUDED] ??= false;

    // Get the table schema
    $schemaManager = $this->connection->createSchemaManager();
    $schema = $schemaManager->listTableDetails($table);

    // Iterate over the data
    foreach ($data as $key => &$value)
    {
      if ($schema->hasColumn($key))
      {
        $valueType = $options[self::NORMALIZE_TYPE_OVERRIDES][$key] ?? $schema->getColumn($key)->getType();
        $value = $this->normalize($value, $valueType);

        if ($options[self::NORMALIZE_TYPE_INCLUDED])
          $value = [self::NORMALIZE_KEY_TYPE => $valueType, self::NORMALIZE_KEY_VALUE => $value];
      }
    }

    // Return the normalized data
    return $data;
  }

  // Convert an array of values from its database representation to its PHP representation
  private function denormalizeArray(string $table, array $data, array $options = []): array
  {
    // Get the default options
    $options[self::NORMALIZE_TYPE_OVERRIDES] ??= [];

    // Get the table schema
    $schemaManager = $this->connection->createSchemaManager();
    $schema = $schemaManager->listTableDetails($table);

    // Iterate over the data
    foreach ($data as $key => &$value)
    {
      if ($schema->hasColumn($key))
      {
        $valueType = $options[self::NORMALIZE_TYPE_OVERRIDES][$key] ?? $schema->getColumn($key)->getType();
        $value = $this->denormalize($value, $valueType);
      }
    }

    // Return the denormalized data
    return $data;
  }


  // Create a where clause
  private static function createWhereClause(QueryBuilder $query, array $where = []): void
  {
    // Return if the order by array is empty
    if (empty($where))
      return;

    // Iterate over the where array
    foreach ($where as $key => $value)
    {
      // If the key is numeric, then use the value as clause, otherwise create an equals clause from the key
      if (!is_int($key))
        $value = "{$key} = {$query->createNamedParameter($value)}";

      // Add the where clause
      $query->andWhere($value);
    }
  }

  // Create an order by clause
  private static function createOrderByClause(QueryBuilder $query, array $orderBy = []): void
  {
    // Return if the order by array is empty
    if (empty($orderBy))
      return;

    // Iterate over the order by array
    foreach ($orderBy as $key => $order)
    {
      // If the key is numeric, then use the value as key
      if (is_int($key))
        [$key, $order] = [$order, 'asc'];

      // Add the order by clause
      $query->addOrderBy($key, $order);
    }
  }
}
