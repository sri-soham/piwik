<?php
/**
 * Piwik - free/libre analytics platform
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 */
namespace Piwik\DataAccess;

use Piwik\Common;
use Piwik\Container\StaticContainer;
use Piwik\Db;
use Piwik\Plugin\Dimension\DimensionMetadataProvider;
use Piwik\Db\Factory;

/**
 * DAO that queries log tables.
 */
class RawLogDao
{
    const DELETE_UNUSED_ACTIONS_TEMP_TABLE_NAME = 'tmp_log_actions_to_keep';

    /**
     * @var DimensionMetadataProvider
     */
    private $dimensionMetadataProvider;

    public function __construct(DimensionMetadataProvider $provider = null)
    {
        $this->dimensionMetadataProvider = $provider ?: StaticContainer::get('Piwik\Plugin\Dimension\DimensionMetadataProvider');
    }

    /**
     * @param array $values
     * @param string $idVisit
     */
    public function updateVisits(array $values, $idVisit)
    {
        $LogVisit = Factory::getDAO('log_visit');
        $LogVisit->updateVisits($values, $idVisit);
    }

    /**
     * @param array $values
     * @param string $idVisit
     */
    public function updateConversions(array $values, $idVisit)
    {
        $LogConversion = Factory::getDAO('log_conversion');
        $LogConversion->updateConversions($values, $idVisit);
    }

    /**
     * @param string $from
     * @param string $to
     * @return int
     */
    public function countVisitsWithDatesLimit($from, $to)
    {
        $LogVisit = Factory::getDAO('log_visit');
        return $LogVisit->countVisitsWithDatesLimit($from, $to);
    }

    /**
     * Iterates over logs in a log table in chunks. Parameters to this function are as backend agnostic
     * as possible w/o dramatically increasing code complexity.
     *
     * @param string $logTable The log table name. Unprefixed, eg, `log_visit`.
     * @param array[] $conditions An array describing the conditions logs must match in the query. Translates to
     *                            the WHERE part of a SELECT statement. Each element must contain three elements:
     *
     *                            * the column name
     *                            * the operator (ie, '=', '<>', '<', etc.)
     *                            * the operand (ie, a value)
     *
     *                            The elements are AND-ed together.
     *
     *                            Example:
     *
     *                            ```
     *                            array(
     *                                array('visit_first_action_time', '>=', ...),
     *                                array('visit_first_action_time', '<', ...)
     *                            )
     *                            ```
     * @param int $iterationStep The number of rows to query at a time.
     * @param callable $callback The callback that processes each chunk of rows.
     */
    public function forAllLogs($logTable, $fields, $conditions, $iterationStep, $callback)
    {
        $idField = $this->getIdFieldForLogTable($logTable);
        list($query, $bind) = $this->createLogIterationQuery($logTable, $idField, $fields, $conditions, $iterationStep);
        $binaryColumns = $this->getBinaryColumns($logTable);
        $Generic = Factory::getGeneric();

        $lastId = 0;
        do {
            $rows = Db::fetchAll($query, array_merge(array($lastId), $bind));
            if (!empty($rows)) {
                $count = count($rows);
                $lastId = $rows[$count - 1][$idField];
                for ($i=0; $i<$count; ++$i) {
                    foreach ($binaryColumns as $bc) {
                        if (!empty($rows[$i][$bc])) {
                            $rows[$i][$bc] = $Generic->db2bin($rows[$i][$bc]);
                        }
                    }
                }

                $callback($rows);
            }
        } while (count($rows) == $iterationStep);
    }

    /**
     * Deletes visits with the supplied IDs from log_visit. This method does not cascade, so rows in other tables w/
     * the same visit ID will still exist.
     *
     * @param int[] $idVisits
     * @return int The number of deleted rows.
     */
    public function deleteVisits($idVisits)
    {
        $LogVisit = Factory::getDAO('log_visit');
        return $LogVisit->deleteVisits($idVisits);
    }

    /**
     * Deletes visit actions for the supplied visit IDs from log_link_visit_action.
     *
     * @param int[] $visitIds
     * @return int The number of deleted rows.
     */
    public function deleteVisitActionsForVisits($visitIds)
    {
        $LogLinkVisitAction = Factory::getDAO('log_link_visit_action');
        return $LogLinkVisitAction->deleteVisitActionsForVisits($visitIds);
    }

    /**
     * Deletes conversions for the supplied visit IDs from log_conversion. This method does not cascade, so
     * conversion items will not be deleted.
     *
     * @param int[] $visitIds
     * @return int The number of deleted rows.
     */
    public function deleteConversions($visitIds)
    {
        $LogConversion = Factory::getDAO('log_conversion');
        return $LogConversion->deleteConversions($visitIds);
    }

    /**
     * Deletes conversion items for the supplied visit IDs from log_conversion_item.
     *
     * @param int[] $visitIds
     * @return int The number of deleted rows.
     */
    public function deleteConversionItems($visitIds)
    {
        $LogConversionItem = Factory::getDAO('log_conversion_item');
        return $LogConversionItem->deleteConversionItems($visitIds);
    }

    /**
     * Deletes all unused entries from the log_action table. This method uses a temporary table to store used
     * actions, and then deletes rows from log_action that are not in this temporary table.
     *
     * Table locking is required to avoid concurrency issues.
     *
     * @throws \Exception If table locking permission is not granted to the current MySQL user.
     */
    public function deleteUnusedLogActions()
    {
        if (!Db::isLockPrivilegeGranted()) {
            throw new \Exception("RawLogDao.deleteUnusedLogActions() requires table locking permission in order to complete without error.");
        }

        $LogAction = Factory::getDAO('log_action');
        $LogAction->purgeUnused($this->dimensionMetadataProvider);
    }

    /**
     * Returns the list of the website IDs that received some visits between the specified timestamp.
     *
     * @param string $fromDateTime
     * @param string $toDateTime
     * @return bool true if there are visits for this site between the given timeframe, false if not
     */
    public function hasSiteVisitsBetweenTimeframe($fromDateTime, $toDateTime, $idSite)
    {
        $LogVisit = Factory::getDAO('log_visit');
        return $LogVisit->hasSiteVisitsBetweenTimeframe($fromDateTime, $toDateTime, $idSite);
    }

    private function getIdFieldForLogTable($logTable)
    {
        switch ($logTable) {
            case 'log_visit':
                return 'idvisit';
            case 'log_link_visit_action':
                return 'idlink_va';
            case 'log_conversion':
                return 'idvisit';
            case 'log_conversion_item':
                return 'idvisit';
            case 'log_action':
                return 'idaction';
            default:
                throw new \InvalidArgumentException("Unknown log table '$logTable'.");
        }
    }

    private function getBinaryColumns($logTable)
    {
        switch ($logTable) {
            case 'log_visit':
                return array('idvisitor', 'config_id', 'location_ip');
            case 'log_link_visit_action':
                return array('idvisitor');
            case 'log_conversion':
                return array('idvisitor');
            case 'log_conversion_item':
                return array('idvisitor');
            case 'log_action':
                return array();
            default:
                throw new \InvalidArgumentException("Unknown log table '$logTable'.");
        }
    }

    // TODO: instead of creating a log query like this, we should re-use segments. to do this, however, there must be a 1-1
    //       mapping for dimensions => segments, and each dimension should automatically have a segment.
    private function createLogIterationQuery($logTable, $idField, $fields, $conditions, $iterationStep)
    {
        $bind = array();

        $sql = "SELECT " . implode(', ', $fields) . " FROM " . Common::prefixTable($logTable) . " WHERE $idField > ?";

        foreach ($conditions as $condition) {
            list($column, $operator, $value) = $condition;

            if (is_array($value)) {
                $sql .= " AND $column IN (" . Common::getSqlStringFieldsArray($value) . ")";

                $bind = array_merge($bind, $value);
            } else {
                $sql .= " AND $column $operator ?";

                $bind[] = $value;
            }
        }

        $sql .= " ORDER BY $idField ASC LIMIT " . (int)$iterationStep;

        return array($sql, $bind);
    }

    private function getInFieldExpressionWithInts($idVisits)
    {
        $sql = "(";

        $isFirst = true;
        foreach ($idVisits as $idVisit) {
            if ($isFirst) {
                $isFirst = false;
            } else {
                $sql .= ', ';
            }

            $sql .= (int)$idVisit;
        }

        $sql .= ")";

        return $sql;
    }
}
