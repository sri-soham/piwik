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
use Piwik\Archive;
use Piwik\Archive\Chunk;
use Piwik\ArchiveProcessor;
use Piwik\ArchiveProcessor\Rules;
use Piwik\Common;
use Piwik\Date;
use Piwik\Db;
use Piwik\Db\Factory;
use Piwik\Period;
use Piwik\Period\Range;
use Piwik\Segment;

/**
 * Data Access object used to query archives
 *
 * A record in the Database for a given report is defined by
 * - idarchive     = unique ID that is associated to all the data of this archive (idsite+period+date)
 * - idsite        = the ID of the website
 * - date1         = starting day of the period
 * - date2         = ending day of the period
 * - period        = integer that defines the period (day/week/etc.). @see period::getId()
 * - ts_archived   = timestamp when the archive was processed (UTC)
 * - name          = the name of the report (ex: uniq_visitors or search_keywords_by_search_engines)
 * - value         = the actual data (a numeric value, or a blob of compressed serialized data)
 *
 */
class ArchiveSelector
{
    const NB_VISITS_RECORD_LOOKED_UP = "nb_visits";

    const NB_VISITS_CONVERTED_RECORD_LOOKED_UP = "nb_visits_converted";

    private static function getModel()
    {
        return Factory::getModel(__NAMESPACE__);
    }

    public static function getArchiveIdAndVisits(ArchiveProcessor\Parameters $params, $minDatetimeArchiveProcessedUTC)
    {
        $idSite       = $params->getSite()->getId();
        $period       = $params->getPeriod()->getId();
        $dateStart    = $params->getPeriod()->getDateStart();
        $dateStartIso = $dateStart->toString('Y-m-d');
        $dateEndIso   = $params->getPeriod()->getDateEnd()->toString('Y-m-d');

        $numericTable = ArchiveTableCreator::getNumericTable($dateStart);

        $minDatetimeIsoArchiveProcessedUTC = null;
        if ($minDatetimeArchiveProcessedUTC) {
            $minDatetimeIsoArchiveProcessedUTC = Date::factory($minDatetimeArchiveProcessedUTC)->getDatetime();
        }

        $requestedPlugin = $params->getRequestedPlugin();
        $segment         = $params->getSegment();
        $plugins = array("VisitsSummary", $requestedPlugin);

        $doneFlags      = Rules::getDoneFlags($plugins, $segment);
        $doneFlagValues = Rules::getSelectableDoneFlagValues();

        $results = self::getModel()->getArchiveIdAndVisits($numericTable, $idSite, $period, $dateStartIso, $dateEndIso, $minDatetimeIsoArchiveProcessedUTC, $doneFlags, $doneFlagValues);

        if (empty($results)) {
            return false;
        }

        $idArchive = self::getMostRecentIdArchiveFromResults($segment, $requestedPlugin, $results);

        $idArchiveVisitsSummary = self::getMostRecentIdArchiveFromResults($segment, "VisitsSummary", $results);

        list($visits, $visitsConverted) = self::getVisitsMetricsFromResults($idArchive, $idArchiveVisitsSummary, $results);

        if (false === $visits && false === $idArchive) {
            return false;
        }

        return array($idArchive, $visits, $visitsConverted);
    }

    protected static function getVisitsMetricsFromResults($idArchive, $idArchiveVisitsSummary, $results)
    {
        $visits = $visitsConverted = false;
        $archiveWithVisitsMetricsWasFound = ($idArchiveVisitsSummary !== false);

        if ($archiveWithVisitsMetricsWasFound) {
            $visits = $visitsConverted = 0;
        }

        foreach ($results as $result) {
            if (in_array($result['idarchive'], array($idArchive, $idArchiveVisitsSummary))) {
                $value = (int)$result['value'];
                if (empty($visits)
                    && $result['name'] == self::NB_VISITS_RECORD_LOOKED_UP
                ) {
                    $visits = $value;
                }
                if (empty($visitsConverted)
                    && $result['name'] == self::NB_VISITS_CONVERTED_RECORD_LOOKED_UP
                ) {
                    $visitsConverted = $value;
                }
            }
        }

        return array($visits, $visitsConverted);
    }

    protected static function getMostRecentIdArchiveFromResults(Segment $segment, $requestedPlugin, $results)
    {
        $idArchive = false;
        $namesRequestedPlugin = Rules::getDoneFlags(array($requestedPlugin), $segment);

        foreach ($results as $result) {
            if ($idArchive === false
                && in_array($result['name'], $namesRequestedPlugin)
            ) {
                $idArchive = $result['idarchive'];
                break;
            }
        }

        return $idArchive;
    }

    /**
     * Queries and returns archive IDs for a set of sites, periods, and a segment.
     *
     * @param array $siteIds
     * @param array $periods
     * @param Segment $segment
     * @param array $plugins List of plugin names for which data is being requested.
     * @return array Archive IDs are grouped by archive name and period range, ie,
     *               array(
     *                   'VisitsSummary.done' => array(
     *                       '2010-01-01' => array(1,2,3)
     *                   )
     *               )
     * @throws
     */
    public static function getArchiveIds($siteIds, $periods, $segment, $plugins)
    {
        if (empty($siteIds)) {
            throw new \Exception("Website IDs could not be read from the request, ie. idSite=");
        }

        foreach ($siteIds as $index => $siteId) {
            $siteIds[$index] = (int) $siteId;
        }

        $monthToPeriods = array();
        foreach ($periods as $period) {
            /** @var Period $period */
            $table = ArchiveTableCreator::getNumericTable($period->getDateStart());
            $monthToPeriods[$table][] = $period;
        }
        $nameCondition = self::getNameCondition($plugins, $segment);

        $Archive = Factory::getDAO('archive');
        $results = $Archive->getArchiveIds($siteIds, $monthToPeriods, $nameCondition);

        return $results;
    }

    /**
     * Queries and returns archive data using a set of archive IDs.
     *
     * @param array $archiveIds The IDs of the archives to get data from.
     * @param array $recordNames The names of the data to retrieve (ie, nb_visits, nb_actions, etc.).
     *                           Note: You CANNOT pass multiple recordnames if $loadAllSubtables=true.
     * @param string $archiveDataType The archive data type (either, 'blob' or 'numeric').
     * @param int|null|string $idSubtable  null if the root blob should be loaded, an integer if a subtable should be
     *                                     loaded and 'all' if all subtables should be loaded.
     * @throws Exception
     * @return array
     */
    public static function getArchiveData($archiveIds, $recordNames, $archiveDataType, $idSubtable)
    {
        $Archive = Factory::getDAO('archive');
        $rows = $Archive->getArchiveData($archiveIds, $recordNames, $archiveDataType, $idSubtable);

        return $rows;
    }

    public static function appendIdSubtable($recordName, $id)
    {
        return $recordName . "_" . $id;
    }

    private static function uncompress($data)
    {
        return @gzuncompress($data);
    }

    /**
     * Returns the SQL condition used to find successfully completed archives that
     * this instance is querying for.
     *
     * @param array $plugins
     * @param Segment $segment
     * @return string
     */
    private static function getNameCondition(array $plugins, Segment $segment)
    {
        // the flags used to tell how the archiving process for a specific archive was completed,
        // if it was completed
        $doneFlags    = Rules::getDoneFlags($plugins, $segment);
        $allDoneFlags = "'" . implode("','", $doneFlags) . "'";

        $possibleValues = Rules::getSelectableDoneFlagValues();

        // create the SQL to find archives that are DONE
        return "((name IN ($allDoneFlags)) AND (value IN (" . implode(',', $possibleValues) . ")))";
    }
}
