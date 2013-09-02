<?php
/**
 * @author  Richard.Gooding
 */

namespace Qubes\BucketProcessor;

class Events
{
  const DISPLAY_REPORT_START = 'bucketprocessor.displayReport.start';
  const DISPLAY_REPORT_END   = 'bucketprocessor.displayReport.end';

  const RANGE_SAVE_CHANGES_START = 'bucketprocessor.rangeSaveChanges.start';
  const RANGE_SAVE_CHANGES_END   = 'bucketprocessor.rangeSaveChanges.end';
  const CLAIM_RANGE_START        = 'bucketprocessor.claimRange.start';
  const CLAIM_RANGE_END          = 'bucketprocessor.claimRange.end';

  const LIST_BUCKET_START = 'bucketprocessor.listBucket.start';
  const LIST_BUCKET_END   = 'bucketprocessor.listBucket.end';
}
