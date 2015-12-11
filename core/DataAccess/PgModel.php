<?php
/**
 * Piwik - free/libre analytics platform
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 */
namespace Piwik\DataAccess;

use Exception;
use Piwik\Common;
use Piwik\Container\StaticContainer;
use Piwik\Db;
use Piwik\Db\Factory;

/**
 * Cleans up outdated archives
 *
 * @package Piwik\DataAccess
 */
class PgModel extends Model
{
    public function insertRecord($tableName, $fields, $record, $name, $value)
    {
        $Generic = Factory::getGeneric();
        $Archive = Factory::getDAO('archive');
        // duplicate idarchives are Ignored, see https://github.com/piwik/piwik/issues/987
        $query = "INSERT INTO " . $tableName . " (" . implode(", ", $fields) . ")
                  VALUES (?,?,?,?,?,?,?,?)";

        $bindSql   = $record;
        $bindSql[] = $name;
        if ($Archive->isBlob($tableName)) {
            $value = $Generic->bin2db($value);
            $value = $Archive->namespaceToUnderscore($value);
            $bindSql[] = $value;
        }
        else {
            $bindSql[] = $value;
        }

        $Generic->insertIgnore($query, $bindSql);

        return true;
    }

    // Valid only for mysql where max length can be set for the value returned
    // GROUP_CONCAT. Since the GROUP_CONCAT max length can be set only in Mysql,
    // we override the function here which does nothing.
    protected function getGroupConcatMaxLen($logger)
    {
    }

    protected function invalidatedArchiveIdRows($archiveTable, $idSites)
    {
        $sql = "SELECT idsite, date1, date2, period, name,
                       STRING_AGG(idarchive || '.' || value, ','  ORDER BY ts_archived DESC) as archives
                  FROM $archiveTable
                 WHERE name LIKE 'done%'
                   AND value IN (" . ArchiveWriter::DONE_INVALIDATED . ','
                                   . ArchiveWriter::DONE_OK . ','
                                   . ArchiveWriter::DONE_OK_TEMPORARY . ")
                   AND idsite IN (" . implode(',', $idSites) . ")
                 GROUP BY idsite, date1, date2, period, name";

        $rows = $this->db->fetchAll($sql);
        if ($this->isBlob($archiveTable)) {
            $Generic = Factory::getGeneric();
            foreach ($rows as &$row) {
                $parts = explode('.', $row['archives']);
                $row['archives'] = $parts[0] . '.' . $Generic->db2bin($parts[1]);
            }
        }

        return $rows;
    }

}
