<?php

namespace Qubes\BucketProcessor;

use Cubex\Cli\CliArgument;
use Cubex\Cli\CliCommand;
use Cubex\Cli\PidFile;
use Cubex\Data\Validator\Validator;
use Qubes\BucketProcessor\Mappers\BucketRange;

/**
 * @author  Richard.Gooding
 */
abstract class BucketProcessorTask extends CliCommand
{
  protected $_pidFile;
  protected $_instanceName = "";
  protected $_rangeProcessor = null;
  private $_displayReport = true;

  /**
   * Get the name of the storage service to use
   *
   * @return string
   */
  protected function _getStorageServiceName()
  {
    return 'storage';
  }
  /**
   * Get the name of the bucket to process
   *
   * @return string
   */
  protected abstract function _getBucketName();
  /**
   * Get the processor for this task
   *
   * @return BatchProcessor
   */
  protected abstract function _getProcessor();

  /**
   * Get the name of the MySQL table used to store the ranges
   *
   * @return string
   */
  protected function _getRangesTableName()
  {
    return 'bucket_ranges';
  }

  protected function _argumentsList()
  {
    return [
      new CliArgument(
        'dry-run',
        'Enable dry run mode, nothing will be written'
      ),
      new CliArgument(
        'instance',
        'Instance name',
        'i',
        CliArgument::VALUE_REQUIRED,
        'name'
      ),
      new CliArgument(
        'build-ranges',
        'Wipe the database table and rebuild the ranges',
        '',
        CliArgument::VALUE_REQUIRED,
        'prefixLength',
        false,
        null,
        Validator::VALIDATE_INT
      ),
      new CliArgument('reset-ranges', 'Reset all ranges'),
      new CliArgument(
        'reset-range',
        'Reset a single range',
        '',
        CliArgument::VALUE_REQUIRED,
        'prefix'
      ),
      new CliArgument(
        'reset-processing',
        'Reset all ranges that are flagged as processing. USE WITH CARE.'
      ),
      new CliArgument(
        'list-failed',
        'List failed ranges',
        '',
        CliArgument::VALUE_OPTIONAL,
        'limit',
        false,
        100,
        Validator::VALIDATE_INT
      ),
      new CliArgument('reset-failed', 'Reset failed ranges'),
      new CliArgument(
        'list-requeued',
        'List ranges that were requeued within the last "mins" minutes',
        '',
        CliArgument::VALUE_OPTIONAL,
        'minutes',
        false,
        30,
        Validator::VALIDATE_INT
      ),
      new CliArgument(
        'batch-size',
        'Maximum number of items to list in the Google bucket in one request',
        '',
        CliArgument::VALUE_REQUIRED,
        'items',
        false,
        1000,
        Validator::VALIDATE_INT
      ),
      new CliArgument('no-report', 'Set this to hide the processing report'),
    ];
  }

  public function init()
  {
    $this->_instanceName = $this->argumentValue('instance', '');
    $this->_logger->setInstanceName($this->_instanceName);

    if($this->argumentIsSet('dry-run'))
    {
      ScriptOptions::setDryRun(true);
    }

    BucketRange::setTableName($this->_getRangesTableName());
  }

  public function execute()
  {
    if($this->argumentValue('no-report'))
    {
      $this->_displayReport = false;
    }

    if($this->argumentIsSet('build-ranges'))
    {
      RangeManager::buildRanges($this->argumentValue('build-ranges'));
    }
    else if($this->argumentIsSet('reset-ranges'))
    {
      RangeManager::resetAllRanges();
    }
    else if($this->argumentIsSet('reset-range'))
    {
      RangeManager::resetRange($this->argumentValue('reset-range'));
    }
    else if($this->argumentIsSet('reset-failed'))
    {
      RangeManager::resetFailedRanges();
    }
    else if($this->argumentIsSet('reset-processing'))
    {
      RangeManager::resetProcessingRanges();
    }
    else if($this->argumentIsSet('list-failed'))
    {
      RangeManager::listFailedRanges($this->argumentValue('list-failed'));
    }
    else if($this->argumentIsSet('list-requeued'))
    {
      RangeManager::listRequeuedRanges($this->argumentValue('list-requeued'));
    }
    else
    {
      $this->_pidFile = new PidFile("", $this->_instanceName);
      $this->_initProcessingRun();
      $this->_getRangeProcessor()->setBatchSize(
        $this->argumentValue('batch-size')
      );
      $this->_getRangeProcessor()->processAllRanges();
    }
  }

  /**
   * Called at the start of the processing run
   */
  protected function _initProcessingRun()
  {
  }

  protected function _getRangeProcessor()
  {
    if($this->_rangeProcessor === null)
    {
      $this->_rangeProcessor = new RangeProcessor(
        $this->_getStorageServiceName(),
        $this->_getBucketName(),
        $this->_getProcessor(),
        $this->_instanceName,
        $this->_displayReport
      );
    }
    return $this->_rangeProcessor;
  }
}
