<?php
/**
 * Piwik - free/libre analytics platform
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 */
namespace Piwik\Plugins\CoreAdminHome\Model;

/**
 * Provides methods to find duplicate actions and fix duplicate action references in tables
 * that reference log_action rows.
 */
class PgDuplicateActionRemover extends DuplicateActionRemover
{
    public function getDuplicateIdActionsFromDB()
    {
        $sql = "SELECT name
                     , COUNT(*) AS count
                     , STRING_AGG(idaction ORDER BY idaction ASC SEPARATOR ',') as idactions
                  FROM " . Common::prefixTable('log_action') . "
              GROUP BY name, hash, type HAVING count > 1";

        return $this->db->fetchAll($sql);
    }

}

