<?php
/**
 * @author  Richard.Gooding
 */

namespace Qubes\BucketProcessor;

use Cubex\Cli\Shell;
use Cubex\Log\Log;
use Cubex\Sprintf\ParseQuery;
use Cubex\Text\TextTable;
use Qubes\BucketProcessor\Mappers\BucketRange;

class RangeManager
{
  /**
   * Maximum number of times to requeue a range before failing it
   */
  const MAX_REQUEUES = 50;

  /**
   * @param int $prefixLength
   */
  public static function buildRanges($prefixLength)
  {
    $db = BucketRange::conn();
    if((new BucketRange())->tableExists())
    {
      $db->query(
        ParseQuery::parse($db, 'DELETE FROM %T', BucketRange::tableName())
      );
    }

    $maxNum = pow(16, $prefixLength);
    for($i = 0; $i < $maxNum; $i++)
    {
      $prefix            = sprintf("%0" . $prefixLength . "x", $i);
      $range             = new BucketRange($prefix);
      $range->processed  = 0;
      $range->processing = 0;
      $range->randomKey  = rand(1, 10000);
      $range->saveChanges();

      Shell::clearLine();
      echo "Building ranges: " . number_format($i + 1) . "/" .
        number_format($maxNum);
    }
    echo "\n";
  }

  public static function resetAllRanges()
  {
    echo "Resetting all ranges...\n";

    $db = BucketRange::conn();

    $db->query(
      ParseQuery::parse(
        $db,
        "UPDATE %T SET lastObject='', hostname='', processing=0, processed=0, " .
        "failed=0, processingTime=0, totalItems=0, processedItems=0, error=''",
        BucketRange::tableName()
      )
    );

    echo $db->affectedRows() . " ranges were reset\n";
  }

  public static function resetRange($prefix)
  {
    $range = new BucketRange($prefix);
    if(!$range->exists())
    {
      throw new \Exception('Range does not exist: ' . $prefix);
    }

    $range->hostname       = null;
    $range->processing     = 0;
    $range->processed      = 0;
    $range->failed         = 0;
    $range->processingTime = 0;
    $range->totalItems     = 0;
    $range->processedItems = 0;
    $range->error          = null;
    $range->requeueCount   = 0;
    $range->saveChanges();
  }

  public static function resetProcessingRanges()
  {
    echo "Resetting processing ranges...\n";
    self::_resetRangesWhere('processing=1');
  }

  public static function resetFailedRanges()
  {
    echo "Resetting failed ranges...\n";
    self::_resetRangesWhere('failed=1');
  }

  private static function _resetRangesWhere($where)
  {
    $db = BucketRange::conn();
    $db->query(
      ParseQuery::parse(
        $db,
        "UPDATE %T SET " .
        "processing=0, processed=0, failed=0, hostname='', instanceName='' " .
        "WHERE " . $where,
        BucketRange::tableName()
      )
    );
    $affectedRows = $db->affectedRows();

    echo "Reset " . $affectedRows . " ranges\n";
  }

  public static function listFailedRanges($limit = 100)
  {
    if(self::_listRangesWhere('failed=1', $limit) == 0)
    {
      echo "No failed ranges were found\n";
    }
  }

  public static function listRequeuedRanges($mins = 30)
  {
    $since = date('Y-m-d H:i:s', time() - ($mins * 60));
    if(self::_listRangesWhere(
      "requeueCount>0 AND processed=0 AND failed=0 AND updatedAt >= '" .
      $since . "'"
    ) == 0)
    {
      echo "No requeued ranges were found\n";
    }
  }

  private static function _listRangesWhere($where, $limit = 0)
  {
    $query = "SELECT prefix, updatedAt, hostname, error FROM %T " .
      "WHERE " . $where;
    if($limit > 0)
    {
      $query .= " LIMIT " . $limit;
    }

    $db = BucketRange::conn();
    $ranges = $db->getRows(
      ParseQuery::parse($db, $query, BucketRange::tableName())
    );
    $numRanges = count($ranges);
    if($numRanges > 0)
    {
      $table = new TextTable();
      $table->setColumnHeaders('prefix', 'updatedAt', 'hostname', 'error');

      foreach($ranges as $range)
      {
        $table->appendRow(
          [$range->prefix, $range->updatedAt, $range->hostname, $range->error]
        );
      }
      echo $table;
    }
    return $numRanges;
  }

  private static function _updateProcessingTime(
    BucketRange $range, $rangeStartTime
  )
  {
    $range->processingTime = time() - $rangeStartTime;
  }

  public static function rangeProcessed(
    BucketRange $range, $rangeStartTime, $rangeData
  )
  {
    self::_updateProcessingTime($range, $rangeStartTime);

    $range->rangeData      = $rangeData;
    $range->processing     = 0;
    $range->processed      = 1;
    $range->saveChanges();
  }

  public static function requeueRange(BucketRange $range, $rangeStartTime, $errorMsg)
  {
    self::_updateProcessingTime($range, $rangeStartTime);
    if($range->requeueCount > self::MAX_REQUEUES)
    {
      self::rangeFailed($range, $rangeStartTime, $errorMsg);
    }

    Log::info('Re-queueing range ' . $range->id());
    $range->requeueCount++;
    $range->error          = $errorMsg;
    $range->processing     = 0;
    $range->processed      = 0;
    $range->failed         = 0;
    $range->processingTime = 0;
    $range->randomKey      = rand(10000, 12000);
    $range->saveChanges();
  }

  public static function rangeFailed(
    BucketRange $range, $rangeStartTime, $errorMsg
  )
  {
    self::_updateProcessingTime($range, $rangeStartTime);
    $range->processing     = 0;
    $range->processed      = 1;
    $range->failed         = 1;
    $range->processingTime = 0;
    $range->error          = $errorMsg;
    $range->saveChanges();
  }
}
