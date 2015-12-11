<?php
/**
 * Piwik - free/libre analytics platform
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 */
namespace Piwik\DataAccess;

use Piwik\Db;
use Piwik\Common;
use Piwik\Db\Factory;

/**
 * Data Access Object for operations dealing with the log_action table.
 */
class Actions
{
    /**
     * Removes a list of actions from the log_action table by ID.
     *
     * @param int[] $idActions
     */
    public function delete($idActions)
    {
        $LogAction = Factory::getDAO('log_action');
        $LogAction->deleteByIdactions($idActions);
    }
}
