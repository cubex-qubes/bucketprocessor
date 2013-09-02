<?php
/**
 * @author  Richard.Gooding
 */

namespace Qubes\BucketProcessor;

use Cubex\Cli\CliLogger;
use Cubex\Cli\Shell;
use Cubex\Events\EventManager;
use Cubex\Helpers\DateTimeHelper;
use Cubex\Log\Log;
use Cubex\Text\ReportTableDecorator;
use Cubex\Text\TextTable;
use Qubes\BucketProcessor\Mappers\BucketRange;

class StatsReporter
{
  private $_reportInterval = 15;
  private $_instanceName;

  private $_startTime;
  public $totalItems;
  public $processedItems;
  public $displayPrettyReport;
  public $rangeTotalItems;
  public $rangeProcessedItems;
  public $rangeStartTime;
  /**
   * @var BucketRange
   */
  public $currentRange;

  public function __construct($instanceName = "")
  {
    $this->_instanceName = $instanceName;
    $this->displayPrettyReport = true;
    $this->resetCounters();
  }

  public function resetCounters()
  {
    $this->_startTime     = microtime(true);
    $this->totalItems     = 0;
    $this->processedItems = 0;
    $this->nextRange();
  }

  public function nextRange(BucketRange $range = null)
  {
    $this->rangeTotalItems = 0;
    $this->rangeProcessedItems = 0;
    $this->currentRange = $range;
    $this->rangeStartTime = time();
  }

  public function addItems($total, $processed)
  {
    $this->totalItems += $total;
    $this->rangeTotalItems += $total;
    $this->processedItems += $processed;
    $this->rangeProcessedItems += $processed;
    if($this->currentRange !== null)
    {
      $this->currentRange->processedItems += $processed;
      $this->currentRange->totalItems += $total;
    }
  }

  public function displayReport($forceLog = false)
  {
    static $lastReportTime = 0;

    $now = microtime(true);

    if($lastReportTime == 0)
    {
      $lastReportTime = $this->rangeStartTime;
    }

    $totalDuration = $now - $this->_startTime;
    if($totalDuration > 0)
    {
      $averageRate = round($this->totalItems / $totalDuration);
    }
    else
    {
      $averageRate = 0;
    }

    $rangeDuration = $now - $this->rangeStartTime;
    if($rangeDuration > 0)
    {
      $currentRate = round($this->rangeTotalItems / $rangeDuration);
    }
    else
    {
      $currentRate = 0;
    }

    $rangeSkipped = $this->rangeTotalItems - $this->rangeProcessedItems;
    $totalSkipped = $this->totalItems - $this->processedItems;

    // Log the stats periodically
    if($forceLog || (($now - $lastReportTime) >= $this->_reportInterval))
    {
      $lastReportTime = $now;
      $this->_logReport(
        $now,
        $this->rangeStartTime,
        $this->rangeProcessedItems,
        $this->rangeTotalItems,
        $totalDuration,
        $currentRate,
        $averageRate,
        $this->currentRange->lastObject
      );
    }

    // Display a nice report
    $prettyReport = $this->_generatePrettyReport(
      $this->currentRange,
      $now,
      $this->rangeStartTime,
      $this->rangeTotalItems,
      $this->rangeProcessedItems,
      $currentRate,
      $rangeSkipped,
      $totalSkipped,
      $totalDuration,
      $averageRate,
      $this->currentRange->lastObject
    );

    if($this->displayPrettyReport)
    {
      Shell::redrawScreen($prettyReport);
    }

    // Store the pretty report in a file
    $logsDir = CliLogger::getDefaultLogPath($this->_instanceName);
    $reportFile = $logsDir . DS . 'report.txt';
    file_put_contents($reportFile, $prettyReport);


    // Store the raw stats
    $rawStats = json_encode(
      [
      'hostname' => $this->currentRange->hostname,
      'instance' => $this->currentRange->instanceName,
      'rangeId' => $this->currentRange->id(),
      'prefix' => $this->currentRange->prefix,
      'timestamp' => $now,
      'rangeStartTime' => $this->rangeStartTime,
      'rangeTotal' => $this->rangeTotalItems,
      'rangeProcessed' => $this->rangeProcessedItems,
      'rangeSkipped' => $rangeSkipped,
      'currentRate' => $currentRate,
      'totalItems' => $this->totalItems,
      'totalProcessed' => $this->processedItems,
      'totalSkipped' => $totalSkipped,
      'totalDuration' => $totalDuration,
      'averageRate' => $averageRate,
      'lastObject' => $this->currentRange->lastObject
      ]
    );

    $rawStatsFile = $logsDir . DS . 'stats.txt';
    file_put_contents($rawStatsFile, $rawStats);
  }

  private function _logReport(
    $now, $rangeStartTime, $rangeProcessed, $rangeTotal,
    $totalDuration, $currentRate, $averageRate, $lastObject
  )
  {
    // Log the stats
    Log::info(
      "CURRENT RANGE: Run time " . DateTimeHelper::secondsToTime(
        $now - $rangeStartTime
      ) .
      ", Processed " . $rangeProcessed . " of " .
      $rangeTotal . " items"
    );
    Log::info(
      "OVERALL: Run time " . DateTimeHelper::secondsToTime($totalDuration) .
      ", Processed " . $this->processedItems . " of " .
      $this->totalItems . " items"
    );
    Log::info(
      "Current rate: " . $currentRate . " items/second, Average rate: " .
      $averageRate . " items/second"
    );
    Log::info("Last object: " . $lastObject);
  }


  private function _generatePrettyReport(
    BucketRange $currentRange, $now, $rangeStartTime, $rangeTotal,
    $rangeProcessed, $currentRate, $rangeSkipped, $totalSkipped, $totalDuration,
    $averageRate, $lastObject
  )
  {
    $t = new TextTable(new ReportTableDecorator());

    $t->appendSubHeading(
      'Current Range ' . Shell::colourText(
        "(" . $currentRange->id() . ")",
        Shell::COLOUR_FOREGROUND_LIGHT_GREY
      )
    );
    $t->appendRow(
      ['Prefix', $currentRange->prefix]
    );
    $t->appendSubHeading('Range statistics');
    $t->appendRows(
      [
      [
        'Processing time',
        DateTimeHelper::secondsToTime($now - $rangeStartTime)
      ],
      ['Total items', number_format($rangeTotal)],
      ['Processed items', number_format($rangeProcessed)],
      ['Skipped', number_format($rangeSkipped)],
      ['Processing rate', number_format($currentRate) . ' items/second']
      ]
    );
    $t->appendSubHeading('Total');
    $t->appendRows(
      [
      ['Processing time', DateTimeHelper::secondsToTime($totalDuration)],
      ['Total items', number_format($this->totalItems)],
      ['Processed items', number_format($this->processedItems)],
      ['Skipped', number_format($totalSkipped)],
      ['Processing rate', number_format($averageRate) . ' items/second']
      ]
    );
    $t->appendSpacer();
    $t->appendRow(['Last object seen', $lastObject]);

    ob_start();
    EventManager::trigger(Events::DISPLAY_REPORT_START);
    echo $t;
    EventManager::trigger(Events::DISPLAY_REPORT_END);
    return ob_get_clean();
  }
}
