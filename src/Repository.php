<?php
namespace Danae\Astral;

use Doctrine\DBAL\Schema\Comparator;
use Doctrine\DBAL\Schema\Schema;
use Symfony\Component\PropertyAccess\PropertyAccess;
use Symfony\Component\Serializer\Exception\NotNormalizableValueException;
use Symfony\Component\Serializer\Normalizer\CacheableSupportsMethodInterface;
use Symfony\Component\Serializer\Normalizer\DenormalizerInterface;
use Symfony\Component\Serializer\Normalizer\NormalizerInterface;
use Symfony\Component\Serializer\Serializer;


// Class that defines a database repository of objects
class Repository
{
  // Key for the fields to include in normalization
  public const NORMALIZE_FIELDS = 'fields';

  // Key for the object to pupulate in normalization
  public const NORMALIZE_OBJECT_TO_POPULATE = 'object_to_populate';

  // Key for the accessor in field options
  public const OPTIONS_ACCESSOR = 'accessor';

  // Key for the normalize mapper in field options
  public const OPTIONS_NORMALIZE_MAPPER = 'normalize_mapper';

  // Key for the denormalize mapper in field options
  public const OPTIONS_DENORMALIZE_MAPPER = 'denormalize_mapper';


  // The database of this repository
  private $database;

  // The table name of this repository
  private $table;

  // The type of objects in this repository
  private $type;

  // The fields of objects in this repository
  private $fields;

  // The primary fields of objects in this repository
  private $primaries;

  // The property accessor of this repository
  private $propertyAccessor;


  // Constructor
  public function __construct(Database $database, string $table, string $type)
  {
    $this->database = $database;
    $this->table = $table;
    $this->type = $type;

    $this->propertyAccessor = PropertyAccess::createPropertyAccessor();
  }

  // Set a field of objects in the repository
  public function field(string $name, string $type, array $options = [])
  {
    $this->fields[$name] = ['type' => $type, 'options' => $options];
  }

  // Set a primary field of objects in the repository
  public function primary(string $name)
  {
    $this->primaries[] = $name;
  }


  // Select multiple objects in the repository
  public function select(array $where = [], array $options = []): array
  {
    // Execute the select query
    $array = $this->database->select($this->table, $where, $options);

    // Denormalize the results
    return array_map(fn($data) => $this->denormalize($data, $this->type), $array);
  }

  // Select an object in the repository
  public function selectOne(array $where = [], array $options = []): ?object
  {
    // Execute the select one query
    $data = $this->database->selectOne($this->table, $where, $options);

    // Denormalize the result, or return null if no result
    return $data !== null ? $this->denormalize($data, $this->type) : null;
  }

  // Insert an object in the repository
  public function insert(object $object): int
  {
    // Normalize the object
    $data = $this->normalize($object);

    // Execute the insert query
    return $this->database->insert($this->table, $data);
  }

  // Update an object in the repository
  public function update(object $object): int
  {
    // Normalize the object and primary keys
    $data = $this->normalize($object);
    $keys = self::filterArrayByKeys($data, $this->primaries);

    // Execute the update query
    return $this->database->update($this->table, $data, $keys);
  }

  // Delete an object in the repository
  public function delete(object $object): int
  {
    // Normalize the object and primary keys
    $data = $this->normalize($object);
    $keys = self::filterArrayByKeys($data, $this->primaries);

    // Execute the delete query
    return $this->database->delete($this->table, $keys);
  }


  // Return a normalized object
  public function normalize($object, string $format = null, array $context = [])
  {
    // Check if the arguments are valid
    if (!is_a($object, $this->type))
      throw NotNormalizableValueException("object must be of type {$this->type} or a subclass thereof");

    // Get the fields to include
    $fields = array_keys($this->fields);
    if (isset($context[self::NORMALIZE_FIELDS]))
      $fields = array_intersect($fields, $context[self::NORMALIZE_FIELDS]);

    // Iterate over the fields and get the values from the object
    $data = [];
    foreach ($fields as $key)
    {
      // Get the options
      $options = $this->fields[$key];
      $accessor = $options['options'][self::OPTIONS_ACCESSOR] ?? $key;

      // Get the value from the object
      $value = $this->propertyAccessor->getValue($object, $accessor);;

      $normalizeMapper = $options['options'][self::OPTIONS_NORMALIZE_MAPPER] ?? null;
      if ($normalizeMapper !== null)
        $value = call_user_func($normalizerMapper, $value);

      $data[$key] = $value;
    }
    return $data;
  }

  // Return a denormalized object
  public function denormalize($data, string $type, string $format = null, array $context = [])
  {
    // Check if the arguments are valid
    if (!is_array($data))
      throw NotNormalizableValueException("data must be an array");
    if (!is_a($type, $this->type, true))
      throw NotNormalizableValueException("type must be {$this->type} or a subclass thereof");

    // Get the fields to include
    $fields = array_keys($this->fields);
    if (isset($context[self::NORMALIZE_FIELDS]))
      $fields = array_intersect($fields, $context[self::NORMALIZE_FIELDS]);

    // Iterate over the fields and set the values to the object
    $object = $context[self::NORMALIZE_OBJECT_TO_POPULATE] ?? new $this->type();
    foreach ($fields as $key)
    {
      if (!isset($data[$key]))
        continue;

      // Get the options
      $options = $this->fields[$key];
      $accessor = $options['options'][self::OPTIONS_ACCESSOR] ?? $key;

      // Set the value to the object
      $value = $data[$key];

      $denormalizeMapper = $options['options'][self::OPTIONS_DENORMALIZE_MAPPER] ?? null;
      if ($denormalizeMapper !== null)
        $value = call_user_func($denormalizeMapper, $value);

      $this->propertyAccessor->setValue($object, $accessor, $value);
    }
    return $object;
  }

  // Check whether the given class is supported for normalization by this normalizer
  public function supportsNormalization($object, string $format = null)
  {
    return is_a($object, $this->type);
  }

  // Checks whether the given class is supported for denormalization by this normalizer
  public function supportsDenormalization($data, string $type, string $format = null)
  {
    return is_a($type, $this->type, true);
  }

  // Check whether the support functions can be cached by type and format
  public function hasCacheableSupportsMethod(): bool
  {
    return true;
  }

  // Create a table for the repository
  public function create()
  {
    // Create a new schema
    $schema = new Schema();

    // Create a table
    $table = $schema->createTable($this->table);
    foreach ($this->fields as $field => $options)
      $table->addColumn($field, $options['type'], $options['options']);
    $table->setPrimaryKey($this->primaries);

    // Get the diff
    $currentSchemaManager = $this->database->getConnection()->createSchemaManager();
    $currentSchema = $currentSchemaManager->createSchema();

    $comparator = new Comparator();
    $diff = $comparator->compareSchemas($currentSchema, $schema);
    $queries = $diff->toSaveSql($this->database->getConnection()->getDatabasePlatform());

    // Execute the queries
    if (!empty($queries))
    {
      $this->database->getConnection()->transactional(function($connection) use ($queries) {
        foreach ($queries as $query)
          $connection->executeQuery($query);
      });
    }
  }


  // Return an array that only includes values with the specified keys
  private static function filterArrayByKeys(array $array, array $keys)
  {
    return array_filter($array, fn($key) => in_array($key, $keys), \ARRAY_FILTER_USE_KEY);
  }
}
