<?php
/**
 * Piwik - Open source web analytics
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 * @category Piwik
 * @package Piwik
 */
namespace Piwik\Db\DAO\Mysql;

use Piwik\Common;
use Piwik\Db\DAO\Base;
use Piwik\Db\Factory;
use Piwik\Metrics;
use Piwik\Tracker\GoalManager;

/**
 * @package Piwik
 * @subpackage Piwik_Db
 */

class LogConversionItem extends Base
{ 
    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    public function getEcommerceDetails($idvisit, $conversion, $actionsLimit)
    {
        $Generic = Factory::getGeneric($this->db);

        $log_action = Common::prefixTable('log_action');

        $sql = 'SELECT log_action_sku.name AS ' . $this->db->quoteIdentifier('itemSKU') . ' '
             . '     , log_action_name.name AS ' . $this->db->quoteIdentifier('itemName') . ' '
             . '     , log_action_category.name AS  ' . $this->db->quoteIdentifier('itemCategory') .' '
             . '     , ' . $Generic->getSqlRevenue('price') . ' AS price '
             . '     , quantity '
             . 'FROM ' . $this->table . ' '
             . 'INNER JOIN ' . $log_action . ' AS log_action_sku '
             . '        ON idaction_sku = log_action_sku.idaction '
             . 'LEFT OUTER JOIN ' . $log_action . ' AS log_action_name '
             . '        ON idaction_name = log_action_name.idaction '
             . 'LEFT OUTER JOIN ' . $log_action . ' AS log_action_category '
             . '        ON idaction_category = log_action_category.idaction '
             . "WHERE idvisit = ? AND idorder = ? AND deleted = '0' "
             . "LIMIT $actionsLimit OFFSET 0";
        $bind = array();
        $bind[] = $idvisit;
        $bind[] = isset($conversion['orderId']) 
                    ? $conversion['orderId']
                    : GoalManager::ITEM_IDORDER_ABANDONED_CART;

        return $this->db->fetchAll($sql, $bind);
    }

    public function getEcommerceItems($field, $bind)
    {
        $Generic = Factory::getGeneric($this->db);
        $revenue = $this->db->quoteIdentifier(Metrics::INDEX_ECOMMERCE_ITEM_REVENUE);
        $quantity = $this->db->quoteIdentifier(Metrics::INDEX_ECOMMERCE_ITEM_QUANTITY);
        $price = $this->db->quoteIdentifier(Metrics::INDEX_ECOMMERCE_ITEM_PRICE);
        $orders = $this->db->quoteIdentifier(Metrics::INDEX_ECOMMERCE_ORDERS);
        $nb_visits = $this->db->quoteIdentifier(Metrics::INDEX_NB_VISITS);

        $ecommerceType = $this->db->quoteIdentifier('ecommerceType');

        $sql = 'SELECT '
             . '    name AS label '
             . '  , ' . $Generic->getSqlRevenue('SUM(quantity * price)') . ' AS ' . $revenue . ' '
             . '  , ' . $Generic->getSqlRevenue('SUM(quantity)') . ' AS ' . $quantity . ' '
             . '  , ' . $Generic->getSqlRevenue('SUM(price)') . ' AS ' . $price . ' '
             . '  , COUNT(DISTINCT idorder) AS ' . $orders . ' '
             . '  , COUNT(DISTINCT idvisit) AS ' . $nb_visits . ' '
             . '  , CASE idorder '
             . "        WHEN '0' THEN " . GoalManager::IDGOAL_CART . ' '
             . '        ELSE ' . GoalManager::IDGOAL_ORDER . ' '
             . "    END AS $ecommerceType "
             . 'FROM ' . $this->table . ' '
             . 'LEFT OUTER JOIN ' . Common::prefixTable('log_action') . ' '
             . "    ON $field = idaction "
             . 'WHERE server_time >= ? '
             . '  AND server_time <= ? '
             . '  AND idsite = ? '
             . "  AND deleted = '0' "
             . "GROUP BY $ecommerceType, label, $field ";

        return $this->db->query($sql, $bind);
    }

    /**
     *  getAllByVisitAndOrder
     *
     *  Fetches all the items in the cart based on the idvisit and idorder
     *  Uses tracker db
     *
     *  @param  int  $visit
     *  @param  string $order1
     *  @param  string $order2
     *
     *  @return array
     */
    public function getAllByVisitAndOrder($visit, $order1, $order2)
    {
        $sql = 'SELECT idaction_sku
                     , idaction_name
                     , idaction_category
                     , idaction_category2
                     , idaction_category3
                     , idaction_category4
                     , idaction_category5
                     , price
                     , quantity
                     , deleted
                     , idorder as idorder_original_value 
                FROM '. $this->table . '
                WHERE idvisit = ?
                    AND (idorder = ? OR idorder = ?)';
        $bind = array($visit, $order1, $order2);

        Common::printDebug("Items found in current cart, for conversion_item (visit,idorder)=" . var_export($bind,true));
        return $this->db->fetchAll($sql, $bind);
    }

    /**
     *  updateByGoalAndItem
     *
     *  Updates a row based on the goal and item.
     *  Uses tracker db

     *  @param array  $goal
     *  @param array  $item
     *  @return void
     */
    public function updateByGoalAndItem($goal, $item)
    {
        $row = $this->getItemRowEnriched($goal, $item);
        Common::printDebug($row);

        $updateParts = $sqlBind = array();
        foreach ($row as $name => $value) {
            $updateParts[] = $name . ' = ? ';
            $sqlBind[] = $value;
        }
        $sql = 'UPDATE ' . $this->table . ' SET ' . implode(', ', $updateParts) . ' '
             . 'WHERE idvisit = ? AND idorder = ? AND idaction_sku = ?';
        $sqlBind[] = $row['idvisit'];
        $sqlBind[] = $item['idorder_original_value'];
        $sqlBind[] = $row['idaction_sku'];

        $this->db->query($sql, $sqlBind);
    }

    public function insertEcommerceItems($goal, $itemsToInsert)
    {
        $sql = 'INSERT INTO ' . $this->table . '(
                   idaction_sku
                 , idaction_name
                 , idaction_category
                 , idaction_category2
                 , idaction_category3
                 , idaction_category4
                 , idaction_category5
                 , price
                 , quantity
                 , deleted
                 , idorder
                 , idsite
                 , idvisitor
                 , server_time
                 , idvisit)
                 VALUES ';
        
        $sql_parts = array();
        $bind = array();
        foreach ($itemsToInsert as $item) {
            $row = array_values($this->getItemRowEnriched($goal, $item));
            $sql_parts[] = ' ( ' . Common::getSqlStringFieldsArray($row) . ' ) ';
            $bind = array_merge($bind, $row);
        }
        
        $sql .= implode(', ', $sql_parts);
        $this->db->query($sql, $bind);

        Common::printDebug($sql);
        Common::printDebug($bind);
    }

    public function getCountByIdvisit($idvisit)
    {
        $sql = 'SELECT COUNT(*) FROM ' . $this->table . ' WHERE idvisit <= ?';
        return (int)$this->db->fetchOne($sql, array($idvisit));
    }

    protected function getItemRowEnriched($goal, $item)
    {
        $Generic = Factory::getGeneric($this->db);

        $class = '\\Piwik\\Tracker\\GoalManager';
        $newRow = array(
            'idaction_sku' => (int)$item[$class::INTERNAL_ITEM_SKU],
            'idaction_name' => (int)$item[$class::INTERNAL_ITEM_NAME],
            'idaction_category' => (int)$item[$class::INTERNAL_ITEM_CATEGORY],
            'idaction_category2' => (int)$item[$class::INTERNAL_ITEM_CATEGORY2],
            'idaction_category3' => (int)$item[$class::INTERNAL_ITEM_CATEGORY3],
            'idaction_category4' => (int)$item[$class::INTERNAL_ITEM_CATEGORY4],
            'idaction_category5' => (int)$item[$class::INTERNAL_ITEM_CATEGORY5],
            'price' => $item[$class::INTERNAL_ITEM_PRICE],
            'quantity' => $item[$class::INTERNAL_ITEM_QUANTITY],
            'deleted' => isset($item['deleted']) ? $item['deleted'] : 0, //deleted
            'idorder' => isset($goal['idorder']) ? $goal['idorder'] : $class::ITEM_IDORDER_ABANDONED_CART, //idorder = 0 in log_conversion_item for carts
            'idsite' => $goal['idsite'],
            'idvisitor' => $Generic->bin2db($goal['idvisitor']),
            'server_time' => $goal['server_time'],
            'idvisit' => $goal['idvisit']
        );
        return $newRow;
    }

    public function getMaxIdvisit()
    {
        $sql = 'SELECT MAX(idvisit) FROM ' . $this->table;
        return $this->db->fetchOne($sql);
    }
}
