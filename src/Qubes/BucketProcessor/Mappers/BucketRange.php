<?php
/**
 * @author  Richard.Gooding
 */

namespace Qubes\BucketProcessor\Mappers;

use Cubex\Events\EventManager;
use Cubex\Mapper\Database\RecordMapper;
use Qubes\BucketProcessor\Events;

/**
 * @index processing,hostname
 * @index processing,processed,randomKey
 * @engine InnoDB
 */
class BucketRange extends RecordMapper
{
  /**
   * @datatype varchar
   */
  public $prefix;
  /**
   * @datatype int
   */
  public $processing = 0;
  /**
   * @datatype int
   */
  public $processed = 0;
  /**
   * @datatype int
   */
  public $failed = 0;
  /**
   * @datatype int
   */
  public $processingTime = 0;
  /**
   * @datatype int
   */
  public $totalItems = 0;
  /**
   * @datatype int
   */
  public $processedItems = 0;
  /**
   * @datatype varchar
   */
  public $error;
  /**
   * @datatype varchar
   */
  public $hostname;
  /**
   * @datatype varchar
   */
  public $instanceName = '';
  /**
   * @datatype varchar
   */
  public $lastObject;
  /**
   * @datatype int
   */
  public $randomKey = 0;
  /**
   * @datatype int
   * @unsigned
   * @notnull
   */
  public $requeueCount = 0;
  /**
   * @datatype text
   */
  public $rangeData = '';

  protected $_schemaType = self::SCHEMA_CAMELCASE;
  protected $_idType = self::ID_MANUAL;

  private static $_overrideTableName = 'bucket_ranges';

  public static function setTableName($tableName)
  {
    self::$_overrideTableName = $tableName;
  }

  public function getIdKey()
  {
    return 'prefix';
  }

  public function getTableName($plural = true)
  {
    return self::$_overrideTableName;
  }

  public function createdAttribute()
  {
    return 'createdAt';
  }
  public function updatedAttribute()
  {
    return 'updatedAt';
  }

  public function saveChanges($validate = false, $processAll = false, $failFirst = false)
  {
    EventManager::trigger(Events::RANGE_SAVE_CHANGES_START);
    parent::saveChanges($validate, $processAll, $failFirst);
    EventManager::trigger(Events::RANGE_SAVE_CHANGES_END);
  }
}
