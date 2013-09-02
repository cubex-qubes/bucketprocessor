<?php
/**
 * @author  Richard.Gooding
 */

namespace Qubes\BucketProcessor;

abstract class BatchProcessor
{
  /**
   * @var mixed
   */
  protected $_rangeData = null;

  /**
   * @param ObjectInfo[] $items
   *
   * @return int The number of items that were processed excluding any
   *             that were skipped
   */
  public abstract function processBatch(array $items);

  /**
   * Return true to stop on all errors
   *
   * @return bool
   */
  public function stopOnErrors()
  {
    return false;
  }

  /**
   * Return true to save progress after each batch
   *
   * @return bool
   */
  public function shouldSaveProgress()
  {
    return false;
  }

  /**
   * Reset the current range data at the start of a new range
   */
  public function resetRangeData()
  {
    $this->_rangeData = null;
  }

  /**
   * Get the current range data
   *
   * @return mixed
   */
  public function getRangeData()
  {
    return $this->_rangeData;
  }

  /**
   * Set the current range data
   *
   * @param mixed $data
   */
  public function setRangeData($data)
  {
    $this->_rangeData = $data;
  }

  /**
   * Check if this exception should cause a range to fail immediately
   *
   * @param \Exception $e
   *
   * @return bool
   */
  public function isFatalException(\Exception $e)
  {
    return true;
  }
}
