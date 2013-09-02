<?php
/**
 * @author  Richard.Gooding
 */

namespace Qubes\BucketProcessor;

use Aws\S3\S3Client;
use Cubex\Foundation\Container;
use Cubex\ServiceManager\IService;
use Cubex\ServiceManager\ServiceConfigTrait;
use Cubex\ServiceManager\ServiceManager;

class StorageService implements IService
{
  use ServiceConfigTrait;

  private $_conn = null;

  public static function getService($name = 'storage')
  {
    $sm = Container::get(Container::SERVICE_MANAGER);
    if($sm instanceof ServiceManager)
    {
      $service = $sm->get($name);
      if($service instanceof StorageService)
      {
        return $service;
      }
      else
      {
        throw new \Exception('Service not an instance of StorageService');
      }
    }
    else
    {
      throw new \Exception('Error getting service manager');
    }
  }

  public function conn()
  {
    if($this->_conn === null)
    {
      $key = $this->config()->getStr('key');
      $secretKey = $this->config()->getStr('secret_key');
      $host = $this->config()->getStr('hostname', 'storage.googleapis.com');
      $useSSL = $this->config()->getBool('use_ssl', false);
      $baseUrl = ($useSSL ? 'https://' : 'http://') . $host;

      $this->_conn = S3Client::factory(
        [
        'key'      => $key,
        'secret'   => $secretKey,
        'base_url' => $baseUrl
        ]
      );
    }
    return $this->_conn;
  }
}
