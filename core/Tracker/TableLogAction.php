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

namespace Piwik\Tracker;

use Piwik\Common;
use Piwik\Db\Factory;
use Piwik\Tracker;


/**
 * This class is used to query Action IDs from the log_action table.
 *
 * A pageview, outlink, download or site search are made of several "Action IDs"
 * For example pageview is idaction_url and idaction_name.
 *
 * @package Piwik\Tracker
 */
class TableLogAction
{
    /**
     * This function will find the idaction from the lookup table piwik_log_action,
     * given an Action name, type, and an optional URL Prefix.
     *
     * This is used to record Page URLs, Page Titles, Ecommerce items SKUs, item names, item categories
     *
     * If the action name does not exist in the lookup table, it will INSERT it
     * @param array $actionsNameAndType Array of one or many (name,type)
     * @return array Returns the an array (Field name => idaction)
     */
    public static function loadIdsAction($actionsNameAndType)
    {
        $LogAction = Factory::getDAO('log_action', Tracker::getDatabase());
        
        // Add url prefix if not set
        foreach($actionsNameAndType as &$action) {
            if(count($action) == 2) {
                $action[] = null;
            }
        }
        $actionIds = $LogAction->queryIdsAction($actionsNameAndType);

        list($queriedIds, $fieldNamesToInsert) = self::processIdsToInsert($actionsNameAndType, $actionIds);

        $insertedIds = self::insertNewIdsAction($actionsNameAndType, $fieldNamesToInsert);

        $queriedIds = $queriedIds + $insertedIds;

        return $queriedIds;
    }

    protected static function insertNewIdsAction($actionsNameAndType, $fieldNamesToInsert)
    {
        $LogAction = Factory::getDAO('log_action', Tracker::getDatabase());

        // Then, we insert all new actions in the lookup table
        $inserted = array();
        foreach ($fieldNamesToInsert as $fieldName) {
            list($name, $type, $urlPrefix) = $actionsNameAndType[$fieldName];
            $actionId = $LogAction->add($name, $type, $urlPrefix);
            $inserted[$fieldName] = $actionId;

            Common::printDebug("Recorded a new action (" . Action::getTypeAsString($type) . ") in the lookup table: " . $name . " (idaction = " . $actionId . ")");
        }
        return $inserted;
    }

    protected static function processIdsToInsert($actionsNameAndType, $actionIds)
    {
        // For the Actions found in the lookup table, add the idaction in the array,
        // If not found in lookup table, queue for INSERT
        $fieldNamesToInsert = $fieldNameToActionId = array();
        foreach ($actionsNameAndType as $fieldName => &$actionNameType) {
            @list($name, $type, $urlPrefix) = $actionNameType;
            if (empty($name)) {
                $fieldNameToActionId[$fieldName] = false;
                continue;
            }

            $found = false;
            foreach ($actionIds as $row) {
                if ($name == $row['name']
                    && $type == $row['type']
                ) {
                    $found = true;

                    $fieldNameToActionId[$fieldName] = $row['idaction'];
                    continue;
                }
            }
            if (!$found) {
                $fieldNamesToInsert[] = $fieldName;
            }
        }
        return array($fieldNameToActionId, $fieldNamesToInsert);
    }
}

