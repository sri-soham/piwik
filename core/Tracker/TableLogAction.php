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
use Piwik\SegmentExpression;
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

    private static function insertNewIdsAction($actionsNameAndType, $fieldNamesToInsert)
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


    /**
     * Convert segment expression to an action ID or an SQL expression.
     *
     * This method is used as a sqlFilter-callback for the segments of this plugin.
     * Usually, these callbacks only return a value that should be compared to the
     * column in the database. In this case, that doesn't work since multiple IDs
     * can match an expression (e.g. "pageUrl=@foo").
     * @param string $valueToMatch
     * @param string $sqlField
     * @param string $matchType
     * @param string $segmentName
     * @throws \Exception
     * @return array|int|string
     */
    public static function getIdActionFromSegment($valueToMatch, $sqlField, $matchType, $segmentName)
    {
        $actionType = self::guessActionTypeFromSegment($segmentName);

        if ($actionType == Action::TYPE_PAGE_URL) {
            // for urls trim protocol and www because it is not recorded in the db
            $valueToMatch = preg_replace('@^http[s]?://(www\.)?@i', '', $valueToMatch);
        }
        $valueToMatch = Common::sanitizeInputValue(Common::unsanitizeInputValue($valueToMatch));

        $LogAction = Factory::getDAO('log_action');
        if ($matchType == SegmentExpression::MATCH_EQUAL
            || $matchType == SegmentExpression::MATCH_NOT_EQUAL
        ) {
            $idAction = $LogAction->getIdaction($valueToMatch, $actionType);
            // if the action is not found, we hack -100 to ensure it tries to match against an integer
            // otherwise binding idaction_name to "false" returns some rows for some reasons (in case &segment=pageTitle==Větrnásssssss)
            if (empty($idAction)) {
                $idAction = -100;
            }
            return $idAction;
        }

        // "name contains $string" match can match several idaction so we cannot return yet an idaction
        // special case
        $sql = $LogAction->sqlIdactionFromSegment($matchType, $actionType);
        return array(
            // mark that the returned value is an sql-expression instead of a literal value
            'SQL'  => $sql,
            'bind' => $valueToMatch,
        );
    }

    /**
     * @param $segmentName
     * @return int
     * @throws \Exception
     */
    private static function guessActionTypeFromSegment($segmentName)
    {
        $exactMatch = array(
            'eventAction' => Action::TYPE_EVENT_ACTION,
            'eventCategory' => Action::TYPE_EVENT_CATEGORY,
            'eventName' => Action::TYPE_EVENT_NAME,
        );
        if(!empty($exactMatch[$segmentName])) {
            return $exactMatch[$segmentName];
        }

        if (stripos($segmentName, 'pageurl') !== false) {
            $actionType = Action::TYPE_PAGE_URL;
            return $actionType;
        } elseif (stripos($segmentName, 'pagetitle') !== false) {
            $actionType = Action::TYPE_PAGE_TITLE;
            return $actionType;
        } elseif (stripos($segmentName, 'sitesearch') !== false) {
            $actionType = Action::TYPE_SITE_SEARCH;
            return $actionType;
        } else {
            throw new \Exception("We cannot guess the action type from the segment $segmentName.");
        }
    }

}

