<?php
/**
 * Piwik - free/libre analytics platform
 *
 * @link     http://piwik.org
 * @license  http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 */
namespace Piwik\Plugins\Dashboard;

use Piwik\Common;
use Piwik\Db;
use Piwik\DbHelper;
use Piwik\WidgetsList;

class PgModel extends Model
{
    /**
     * Saves the layout as default
     */
    public function createOrUpdateDashboard($login, $idDashboard, $layout)
    {
        $sql = 'SELECT * FROM ' . $this->table . ' '
             . 'WHERE login = ? AND iddashboard = ? FOR UPDATE';
        $row = $this->db->query($sql, array($login, $idDashboard));
        if ($row) {
            $sql = 'UPDATE ' . $this->table . ' SET '
                 . ' layout = ? '
                 . 'WHERE login = ? AND iddashboard = ?';
            $bind = array($layout, $login, $idDashboard);
        }
        else {
            $sql = 'INSERT INTO ' . $this->table . '(login, iddashboard, layout) '
                 . 'VALUES (?, ?, ?)';
            $bind = array($login, $idDashboard, $layout);
        }
        $this->db->query($sql, $bind);
    }

    /**
     * Records the layout in the DB for the given user.
     *
     * @param string $login
     * @param int $idDashboard
     * @param string $layout
     */
    public function updateLayoutForUser($login, $idDashboard, $layout)
    {
        $this->createOrUpdateDashboard($login, $idDashboard, $layout);
    }

}

