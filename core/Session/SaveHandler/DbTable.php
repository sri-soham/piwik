<?php
/**
 * Piwik - free/libre analytics platform
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 */

namespace Piwik\Session\SaveHandler;

use Piwik\Db;
use Piwik\Db\Factory;
use Zend_Session;
use Zend_Session_SaveHandler_Interface;

/**
 * Database-backed session save handler
 *
 */
class DbTable implements Zend_Session_SaveHandler_Interface
{
    protected $config;
    protected $maxLifetime;
    protected $model;

    /**
     * @param array $config
     */
    public function __construct($config)
    {
        $this->config = $config;
        $this->maxLifetime = ini_get('session.gc_maxlifetime');
        $this->model = Factory::getModel(__NAMESPACE__);
        $this->model->setConfig($config);
    }

    /**
     * Destructor
     *
     * @return void
     */
    public function __destruct()
    {
        Zend_Session::writeClose();
    }

    /**
     * Open Session - retrieve resources
     *
     * @param string $save_path
     * @param string $name
     * @return boolean
     */
    public function open($save_path, $name)
    {
        Db::get()->getConnection();

        return true;
    }

    /**
     * Close Session - free resources
     *
     * @return boolean
     */
    public function close()
    {
        return true;
    }

    /**
     * Read session data
     *
     * @param string $id
     * @return string
     */
    public function read($id)
    {
        return $this->model->read($id);
    }

    /**
     * Write Session - commit data to resource
     *
     * @param string $id
     * @param mixed $data
     * @return boolean
     */
    public function write($id, $data)
    {
        return $this->model->write($id, $data, $this->maxLifetime);
    }

    /**
     * Destroy Session - remove data from resource for
     * given session id
     *
     * @param string $id
     * @return boolean
     */
    public function destroy($id)
    {
        return $this->model->destroy($id);
    }

    /**
     * Garbage Collection - remove old session data older
     * than $maxlifetime (in seconds)
     *
     * @param int $maxlifetime timestamp in seconds
     * @return bool  always true
     */
    public function gc($maxlifetime)
    {
        return $this->model->gc($maxlifetime);
    }
}
