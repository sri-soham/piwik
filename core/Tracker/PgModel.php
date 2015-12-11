<?php
/**
 * Piwik - free/libre analytics platform
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 */
namespace Piwik\Tracker;

use Exception;
use Piwik\Common;
use Piwik\Db\Factory;
use Piwik\Tracker;

class PgModel extends Model
{

    public function createAction($visitAction)
    {
        $fields = implode(", ", array_keys($visitAction));
        $values = Common::getSqlStringFieldsArray($visitAction);
        $table  = Common::prefixTable('log_link_visit_action');
        $db = $this->getDb();
        $Generic = Factory::getGeneric($db);

        $sql  = "INSERT INTO $table ($fields) VALUES ($values)";
        if (!empty($visitAction['value'])) {
            $visitAction['value'] = $Generic->bin2db($visitAction['value']);
        }
        $bind = array_values($visitAction);

        $seq = $table . '_idlink_va';
        $db->query($sql, $bind);

        $id = $db->lastInsertId($seq);

        return $id;
    }

    public function createConversion($conversion)
    {
        $fields     = implode(", ", array_keys($conversion));
        $bindFields = Common::getSqlStringFieldsArray($conversion);
        $table      = Common::prefixTable('log_conversion');
        $db = $this->getDb();
        $Generic = Factory::getGeneric($db);

        $sql    = "INSERT INTO $table ($fields) VALUES ($bindFields) ";
        if (!empty($conversion['idvisitor'])) {
            $conversion['idvisitor'] = $Generic->bin2db($conversion['idvisitor']);
        }
        $bind   = array_values($conversion);

        $result = $Generic->insertIgnore($sql, $bind);

        // If a record was inserted, we return true
        return $db->rowCount($result) > 0;
    }

    public function getIdActionMatchingNameAndType($name, $type)
    {
        $sql  = $this->getSqlSelectActionId();
        $bind = array(Common::getCrc32($name), $name, $type);

        $idAction = $this->getDb()->fetchOne($sql, $bind);

        return $idAction;
    }

    /**
     * Returns the IDs for multiple actions based on name + type values.
     *
     * @param array $actionsNameAndType Array like `array( array('name' => '...', 'type' => 1), ... )`
     * @return array|false Array of DB rows w/ columns: **idaction**, **type**, **name**.
     */
    public function getIdsAction($actionsNameAndType)
    {
        $sql = "SELECT MIN(idaction) as idaction, type, name FROM " . Common::prefixTable('log_action')
             . " WHERE";
        $bind = array();

        $i = 0;
        foreach ($actionsNameAndType as $actionNameType) {
            $name = $actionNameType['name'];

            if (empty($name)) {
                continue;
            }

            if ($i > 0) {
                $sql .= " OR";
            }

            $sql .= " " . $this->getSqlConditionToMatchSingleAction() . " ";

            $bind[] = Common::getCrc32($name);
            $bind[] = $name;
            $bind[] = $actionNameType['type'];
            $i++;
        }

        $sql .= " GROUP BY type, name";

        // Case URL & Title are empty
        if (empty($bind)) {
            return false;
        }

        $actionIds = $this->getDb()->fetchAll($sql, $bind);

        return $actionIds;
    }

    protected function insertNewAction($name, $type, $urlPrefix)
    {
        $table = Common::prefixTable('log_action');
        $sql   = "INSERT INTO $table (name, hash, type, url_prefix) VALUES (?,?,?,?)";

        $db = $this->getDb();
        $db->query($sql, array($name, Common::getCrc32($name), $type, $urlPrefix));

        $seq = $table . '_idaction';
        $actionId = $db->lastInsertId($seq);

        return $actionId;
    }

    protected function getSqlConditionToMatchSingleAction()
    {
        return "( hash = ? AND name = ? AND type = ? )";
    }
}
