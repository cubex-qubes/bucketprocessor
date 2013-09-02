<?php
/**
 * @author  Richard.Gooding
 */

namespace Qubes\BucketProcessor;

use Aws\S3\S3Client;
use Cubex\Events\EventManager;
use Cubex\Log\Log;
use Cubex\Mapper\Database\RecordCollection;
use Cubex\Sprintf\ParseQuery;
use Qubes\BucketProcessor\Mappers\BucketRange;

class RangeProcessor
{
  protected $_hostname;
  protected $_instanceName;
  protected $_statsReporter;
  protected $_processor;
  protected $_batchSize = 1000;
  protected $_bucketName;
  private $_gcs = null;
  protected $_storageServiceName;

  public function __construct(
    $storageServiceName,
    $bucketName,
    BatchProcessor $processor,
    $instanceName = "",
    $displayReport = true
  )
  {
    $this->_storageServiceName = $storageServiceName;
    $this->_bucketName = $bucketName;
    $this->_instanceName = $instanceName;
    $this->_hostname = gethostname();
    $this->_statsReporter = new StatsReporter($this->_instanceName);
    $this->_statsReporter->displayPrettyReport = $displayReport;
    $this->_processor = $processor;
  }

  public function setBatchSize($batchSize)
  {
    $this->_batchSize = $batchSize;
  }

  /**
   * @return BucketRange
   */
  protected function _claimNextFreeRange()
  {
    $range = false;
    $db    = BucketRange::conn();

    EventManager::trigger(Events::CLAIM_RANGE_START);

    // Check for an already-flagged range
    $coll = new RecordCollection(new BucketRange());
    $coll->loadWhere(
      [
      'processing'   => 1,
      'hostname'     => $this->_hostname,
      'instanceName' => $this->_instanceName
      ]
    )->limit(1);

    if($coll->count() > 0)
    {
      $range = $coll->first();
    }
    else
    {
      $res = $db->query(
        ParseQuery::parse(
          $db,
          "UPDATE %T SET processing=1, hostname=%s, instanceName=%s " .
          "WHERE processing=0 AND processed=0 ORDER BY randomKey LIMIT 1",
          BucketRange::tableName(),
          $this->_hostname,
          $this->_instanceName
        )
      );

      if($res)
      {
        $range = BucketRange::loadWhere(
          [
          'processing'   => 1,
          'hostname'     => $this->_hostname,
          'instanceName' => $this->_instanceName
          ]
        );
      }
    }
    EventManager::trigger(Events::CLAIM_RANGE_END);

    return $range;
  }

  /**
   * @return S3Client
   */
  protected function _getGCS()
  {
    if($this->_gcs === null)
    {
      $this->_gcs =
        StorageService::getService($this->_storageServiceName)->conn();
    }
    return $this->_gcs;
  }

  public function processAllRanges()
  {
    $this->_statsReporter->resetCounters();
    while(true)
    {
      $range = $this->_claimNextFreeRange();
      if(! $range)
      {
        Log::notice('No more ranges to process');
        break;
      }

      $this->processRange($range);
    }
  }

  public function processRange(BucketRange $range)
  {
    Log::info('Processing range ' . $range->id());

    $this->_statsReporter->nextRange($range);
    $this->_processor->resetRangeData();

    $listOpts = [
      'Bucket'  => $this->_bucketName,
      'MaxKeys' => $this->_batchSize,
      'Prefix'  => $range->prefix
    ];

    try
    {
      if($this->_processor->shouldSaveProgress() &&
        (!empty($range->lastObject))
      )
      {
        $lastObject = $range->lastObject;
        Log::info('Continuing from key ' . $lastObject);
      }
      else
      {
        $lastObject = "";
      }

      $gcs = $this->_getGCS();

      $finished = false;
      while(!$finished)
      {
        if($lastObject != "")
        {
          $listOpts['Marker'] = $lastObject;
        }
        EventManager::trigger(Events::LIST_BUCKET_START);
        $objects      = $gcs->listObjects($listOpts);
        EventManager::trigger(Events::LIST_BUCKET_END);
        $numObjects = count($objects['Contents']);
        if($numObjects > 0)
        {
          $items = [];
          foreach($objects['Contents'] as $object)
          {
            $items[] = new ObjectInfo(
              $object['Key'], $object['Size'],
              $object['ETag'], $object['LastModified']
            );
          }
          $batchProcessed = $this->_processor->processBatch($items);

          $lastObjectObj = end($items);
          $lastObject = $lastObjectObj->objectKey;
          $range->lastObject = $lastObject;
          $this->_statsReporter->addItems($numObjects, $batchProcessed);

          if($this->_processor->shouldSaveProgress())
          {
            $range->rangeData = $this->_processor->getRangeData();
            $range->saveChanges();
          }
        }
        else
        {
          Log::info(
            'Finished processing range ' . $range->prefix .
            ' (' . $this->_statsReporter->rangeTotalItems . ' total items)'
          );
          $finished = true;
        }

        $this->_statsReporter->displayReport();
      }

      RangeManager::rangeProcessed(
        $range,
        $this->_statsReporter->rangeStartTime,
        $this->_processor->getRangeData()
      );
    }
    catch(\Exception $e)
    {
      $msg = "Code " . $e->getCode();
      $exMsg = $e->getMessage();
      if($exMsg != "")
      {
        $msg .= ": " . $exMsg;
      }
      Log::error(
        'Error processing range: ' . $msg . "\n\nBacktrace:\n" .
        $e->getTraceAsString()
      );

      if($this->_processor->isFatalException($e))
      {
        RangeManager::rangeFailed(
          $range, $this->_statsReporter->rangeStartTime, $msg
        );
      }
      else
      {
        RangeManager::requeueRange(
          $range, $this->_statsReporter->rangeStartTime, $msg
        );
      }
    }
  }
}
