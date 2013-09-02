<?php
/**
 * @author  Richard.Gooding
 */

namespace Qubes\BucketProcessor;

class ObjectInfo
{
  public $objectKey;
  public $filename;
  public $path;
  public $size;
  public $md5;
  public $lastModified;

  public function __construct($objectKey, $size, $md5, $lastModifiedStr)
  {
    $this->objectKey = $objectKey;
    $parts = explode('/', $objectKey);
    $this->filename = array_pop($parts);
    $this->path = implode('/', $parts);
    $this->size = $size;
    $this->md5 = $md5;
    $this->lastModified = strtotime($lastModifiedStr);
  }
}
