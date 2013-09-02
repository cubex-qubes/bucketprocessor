<?php
/**
 * @author  Richard.Gooding
 */

namespace Qubes\BucketProcessor;

class ScriptOptions
{
  private static $_dryRun = false;

  public static function setDryRun($dryRun)
  {
    self::$_dryRun = $dryRun;
  }

  public static function dryRun()
  {
    return self::$_dryRun;
  }
}
