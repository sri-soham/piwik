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

/**
 * The archive object is used to query specific data for a day or a period of statistics for a given website.
 *
 * Example:
 * <pre>
 *        $archive = Piwik_Archive::build($idSite = 1, $period = 'week', '2008-03-08' );
 *        $dataTable = $archive->getDataTable('Provider_hostnameExt');
 *        $dataTable->queueFilter('ReplaceColumnNames');
 *        return $dataTable;
 * </pre>
 *
 * Example bis:
 * <pre>
 *        $archive = Piwik_Archive::build($idSite = 3, $period = 'day', $date = 'today' );
 *        $nbVisits = $archive->getNumeric('nb_visits');
 *        return $nbVisits;
 * </pre>
 *
 * If the requested statistics are not yet processed, Archive uses ArchiveProcessing to archive the statistics.
 *
 * @package Piwik
 * @subpackage Piwik_Archive
 */
class Piwik_Archive
{
    /**
     * When saving DataTables in the DB, we sometimes replace the columns name by these IDs so we save up lots of bytes
     * Eg. INDEX_NB_UNIQ_VISITORS is an integer: 4 bytes, but 'nb_uniq_visitors' is 16 bytes at least
     * (in php it's actually even much more)
     *
     */
    const INDEX_NB_UNIQ_VISITORS = 1;
    const INDEX_NB_VISITS = 2;
    const INDEX_NB_ACTIONS = 3;
    const INDEX_MAX_ACTIONS = 4;
    const INDEX_SUM_VISIT_LENGTH = 5;
    const INDEX_BOUNCE_COUNT = 6;
    const INDEX_NB_VISITS_CONVERTED = 7;
    const INDEX_NB_CONVERSIONS = 8;
    const INDEX_REVENUE = 9;
    const INDEX_GOALS = 10;
    const INDEX_SUM_DAILY_NB_UNIQ_VISITORS = 11;

    // Specific to the Actions reports
    const INDEX_PAGE_NB_HITS = 12;
    const INDEX_PAGE_SUM_TIME_SPENT = 13;

    const INDEX_PAGE_EXIT_NB_UNIQ_VISITORS = 14;
    const INDEX_PAGE_EXIT_NB_VISITS = 15;
    const INDEX_PAGE_EXIT_SUM_DAILY_NB_UNIQ_VISITORS = 16;

    const INDEX_PAGE_ENTRY_NB_UNIQ_VISITORS = 17;
    const INDEX_PAGE_ENTRY_SUM_DAILY_NB_UNIQ_VISITORS = 18;
    const INDEX_PAGE_ENTRY_NB_VISITS = 19;
    const INDEX_PAGE_ENTRY_NB_ACTIONS = 20;
    const INDEX_PAGE_ENTRY_SUM_VISIT_LENGTH = 21;
    const INDEX_PAGE_ENTRY_BOUNCE_COUNT = 22;

    // Ecommerce Items reports
    const INDEX_ECOMMERCE_ITEM_REVENUE = 23;
    const INDEX_ECOMMERCE_ITEM_QUANTITY = 24;
    const INDEX_ECOMMERCE_ITEM_PRICE = 25;
    const INDEX_ECOMMERCE_ORDERS = 26;
    const INDEX_ECOMMERCE_ITEM_PRICE_VIEWED = 27;

    // Site Search
    const INDEX_SITE_SEARCH_HAS_NO_RESULT = 28;
    const INDEX_PAGE_IS_FOLLOWING_SITE_SEARCH_NB_HITS = 29;

    // Performance Analytics
    const INDEX_PAGE_SUM_TIME_GENERATION = 30;
    const INDEX_PAGE_NB_HITS_WITH_TIME_GENERATION = 31;
    const INDEX_PAGE_MIN_TIME_GENERATION = 32;
    const INDEX_PAGE_MAX_TIME_GENERATION = 33;

    // Goal reports
    const INDEX_GOAL_NB_CONVERSIONS = 1;
    const INDEX_GOAL_REVENUE = 2;
    const INDEX_GOAL_NB_VISITS_CONVERTED = 3;

    const INDEX_GOAL_ECOMMERCE_REVENUE_SUBTOTAL = 4;
    const INDEX_GOAL_ECOMMERCE_REVENUE_TAX = 5;
    const INDEX_GOAL_ECOMMERCE_REVENUE_SHIPPING = 6;
    const INDEX_GOAL_ECOMMERCE_REVENUE_DISCOUNT = 7;
    const INDEX_GOAL_ECOMMERCE_ITEMS = 8;

    public static $mappingFromIdToName = array(
        Piwik_Archive::INDEX_NB_UNIQ_VISITORS                      => 'nb_uniq_visitors',
        Piwik_Archive::INDEX_NB_VISITS                             => 'nb_visits',
        Piwik_Archive::INDEX_NB_ACTIONS                            => 'nb_actions',
        Piwik_Archive::INDEX_MAX_ACTIONS                           => 'max_actions',
        Piwik_Archive::INDEX_SUM_VISIT_LENGTH                      => 'sum_visit_length',
        Piwik_Archive::INDEX_BOUNCE_COUNT                          => 'bounce_count',
        Piwik_Archive::INDEX_NB_VISITS_CONVERTED                   => 'nb_visits_converted',
        Piwik_Archive::INDEX_NB_CONVERSIONS                        => 'nb_conversions',
        Piwik_Archive::INDEX_REVENUE                               => 'revenue',
        Piwik_Archive::INDEX_GOALS                                 => 'goals',
        Piwik_Archive::INDEX_SUM_DAILY_NB_UNIQ_VISITORS            => 'sum_daily_nb_uniq_visitors',

        // Actions metrics
        Piwik_Archive::INDEX_PAGE_NB_HITS                          => 'nb_hits',
        Piwik_Archive::INDEX_PAGE_SUM_TIME_SPENT                   => 'sum_time_spent',
        Piwik_Archive::INDEX_PAGE_SUM_TIME_GENERATION              => 'sum_time_generation',
        Piwik_Archive::INDEX_PAGE_NB_HITS_WITH_TIME_GENERATION     => 'nb_hits_with_time_generation',
        Piwik_Archive::INDEX_PAGE_MIN_TIME_GENERATION              => 'min_time_generation',
        Piwik_Archive::INDEX_PAGE_MAX_TIME_GENERATION              => 'max_time_generation',

        Piwik_Archive::INDEX_PAGE_EXIT_NB_UNIQ_VISITORS            => 'exit_nb_uniq_visitors',
        Piwik_Archive::INDEX_PAGE_EXIT_NB_VISITS                   => 'exit_nb_visits',
        Piwik_Archive::INDEX_PAGE_EXIT_SUM_DAILY_NB_UNIQ_VISITORS  => 'sum_daily_exit_nb_uniq_visitors',

        Piwik_Archive::INDEX_PAGE_ENTRY_NB_UNIQ_VISITORS           => 'entry_nb_uniq_visitors',
        Piwik_Archive::INDEX_PAGE_ENTRY_SUM_DAILY_NB_UNIQ_VISITORS => 'sum_daily_entry_nb_uniq_visitors',
        Piwik_Archive::INDEX_PAGE_ENTRY_NB_VISITS                  => 'entry_nb_visits',
        Piwik_Archive::INDEX_PAGE_ENTRY_NB_ACTIONS                 => 'entry_nb_actions',
        Piwik_Archive::INDEX_PAGE_ENTRY_SUM_VISIT_LENGTH           => 'entry_sum_visit_length',
        Piwik_Archive::INDEX_PAGE_ENTRY_BOUNCE_COUNT               => 'entry_bounce_count',
        Piwik_Archive::INDEX_PAGE_IS_FOLLOWING_SITE_SEARCH_NB_HITS => 'nb_hits_following_search',

        // Items reports metrics
        Piwik_Archive::INDEX_ECOMMERCE_ITEM_REVENUE                => 'revenue',
        Piwik_Archive::INDEX_ECOMMERCE_ITEM_QUANTITY               => 'quantity',
        Piwik_Archive::INDEX_ECOMMERCE_ITEM_PRICE                  => 'price',
        Piwik_Archive::INDEX_ECOMMERCE_ITEM_PRICE_VIEWED           => 'price_viewed',
        Piwik_Archive::INDEX_ECOMMERCE_ORDERS                      => 'orders',
    );

    public static $mappingFromIdToNameGoal = array(
        Piwik_Archive::INDEX_GOAL_NB_CONVERSIONS             => 'nb_conversions',
        Piwik_Archive::INDEX_GOAL_NB_VISITS_CONVERTED        => 'nb_visits_converted',
        Piwik_Archive::INDEX_GOAL_REVENUE                    => 'revenue',
        Piwik_Archive::INDEX_GOAL_ECOMMERCE_REVENUE_SUBTOTAL => 'revenue_subtotal',
        Piwik_Archive::INDEX_GOAL_ECOMMERCE_REVENUE_TAX      => 'revenue_tax',
        Piwik_Archive::INDEX_GOAL_ECOMMERCE_REVENUE_SHIPPING => 'revenue_shipping',
        Piwik_Archive::INDEX_GOAL_ECOMMERCE_REVENUE_DISCOUNT => 'revenue_discount',
        Piwik_Archive::INDEX_GOAL_ECOMMERCE_ITEMS            => 'items',
    );

    /**
     * string indexed column name => Integer indexed column name
     * @var array
     */
    public static $mappingFromNameToId = array(
        'nb_uniq_visitors'           => Piwik_Archive::INDEX_NB_UNIQ_VISITORS,
        'nb_visits'                  => Piwik_Archive::INDEX_NB_VISITS,
        'nb_actions'                 => Piwik_Archive::INDEX_NB_ACTIONS,
        'max_actions'                => Piwik_Archive::INDEX_MAX_ACTIONS,
        'sum_visit_length'           => Piwik_Archive::INDEX_SUM_VISIT_LENGTH,
        'bounce_count'               => Piwik_Archive::INDEX_BOUNCE_COUNT,
        'nb_visits_converted'        => Piwik_Archive::INDEX_NB_VISITS_CONVERTED,
        'nb_conversions'             => Piwik_Archive::INDEX_NB_CONVERSIONS,
        'revenue'                    => Piwik_Archive::INDEX_REVENUE,
        'goals'                      => Piwik_Archive::INDEX_GOALS,
        'sum_daily_nb_uniq_visitors' => Piwik_Archive::INDEX_SUM_DAILY_NB_UNIQ_VISITORS,
    );

    /**
     * Metrics calculated and archived by the Actions plugin.
     *
     * @var array
     */
    public static $actionsMetrics = array(
        'nb_pageviews',
        'nb_uniq_pageviews',
        'nb_downloads',
        'nb_uniq_downloads',
        'nb_outlinks',
        'nb_uniq_outlinks',
        'nb_searches',
        'nb_keywords',
        'nb_hits',
        'nb_hits_following_search',
    );

    const LABEL_ECOMMERCE_CART = 'ecommerceAbandonedCart';
    const LABEL_ECOMMERCE_ORDER = 'ecommerceOrder';
    
    /**
     * TODO
     */
    private $siteIds;
    
    /**
     * TODO
     */
    public $periods;
    
    /**
     * Segment applied to the visits set
     * @var Piwik_Segment
     */
    private $segment;
    
    /**
     * TODO
     */
    private $idarchives = null;
    
    /**
     * TODO
     */
    private $forceIndexedBySite;
    
    /**
     * TODO
     */
    private $forceIndexedByDate;
    
    /**
     * TODO
     */
    private $blobCache = array();
    
    /**
     * Constructor.
     * TODO
     */
    public function __construct($siteIds, $periods, Piwik_Segment $segment, $forceIndexedBySite = false,
                                  $forceIndexedByDate = false)
    {
        if (!is_array($siteIds)) {
            $siteIds = array($siteIds);
        }
        if (!is_array($periods)) {
            $periods = array($periods);
        }
        
        if (empty($siteIds)) { // TODO move to helper
            throw new Exception("Piwik_Archive::__construct: \$siteIds is empty.");
        }
        
        if (empty($periods)) {
            throw new Exception("Piwik_Archive::__construct: \$periods is empty.");
        }
        
        $this->siteIds = $siteIds;
        
        $this->periods = array();
        foreach ($periods as $period) {
            $this->periods[$period->getRangeString()] = $period;
            // TODO: getRangeString() is the key, so don't need to call it in other places
        }
        
        $this->segment = $segment;
        $this->forceIndexedBySite = $forceIndexedBySite;
        $this->forceIndexedByDate = $forceIndexedByDate;
    }
    
    /**
     * TODO
     */
    public function __destruct()
    {
        $this->periods = null;
        $this->siteIds = null;
        $this->segment = null;
        $this->idarchives = null;
        $this->blobCache = null;
    }

    /**
     * Builds an Archive object or returns the same archive if previously built.
     *
     * @param int|string $idSite                 integer, or comma separated list of integer
     * @param string $period                 'week' 'day' etc.
     * @param Piwik_Date|string $strDate                'YYYY-MM-DD' or magic keywords 'today' @see Piwik_Date::factory()
     * @param bool|string $segment                Segment definition - defaults to false for Backward Compatibility
     * @param bool|string $_restrictSitesToLogin  Used only when running as a scheduled task
     * @return Piwik_Archive
     * TODO modify
     */
    public static function build($idSite, $period, $strDate, $segment = false, $_restrictSitesToLogin = false)
    {
        $forceIndexedBySite = false;
        $forceIndexedByDate = false;
        
        // determine site IDs to query from
        if ($idSite === 'all') {// TODO: this should be done in constructor
            $sites = Piwik_SitesManager_API::getInstance()->getSitesIdWithAtLeastViewAccess($_restrictSitesToLogin);
            $forceIndexedBySite = true;
        } else {
            if (is_array($idSite)) {
                $forceIndexedBySite = true;
            }
            $sites = Piwik_Site::getIdSitesFromIdSitesString($idSite);
        }
        
        // determine timezone of dates. if more than one site, use UTC, otherwise use the site's timezone.
        // TODO: should use each site's timezone.
        if (count($sites) == 1) {
            $oSite = new Piwik_Site($sites[0]);
            $tz = $oSite->getTimezone();
        } else {
            $tz =  'UTC';
        }
        
        // if a period date string is detected: either 'last30', 'previous10' or 'YYYY-MM-DD,YYYY-MM-DD'
        if (is_string($strDate)
            && self::isMultiplePeriod($strDate, $period)
        ) {
            $oPeriod = new Piwik_Period_Range($period, $strDate, $tz);
            $allPeriods = $oPeriod->getSubperiods();
            $forceIndexedByDate = true;
        } else {
            $oSite = new Piwik_Site(reset($sites));
            $oPeriod = Piwik_Archive::makePeriodFromQueryParams($oSite, $period, $strDate);
            $allPeriods = array($oPeriod);
        }
        
        return new Piwik_Archive(
            $sites, $allPeriods, new Piwik_Segment($segment, $sites), $forceIndexedBySite, $forceIndexedByDate);
    }

    /**
     * Creates a period instance using a Piwik_Site instance and two strings describing
     * the period & date.
     *
     * @param Piwik_Site $site
     * @param string $strPeriod The period string: day, week, month, year, range
     * @param string $strDate The date or date range string.
     * @return Piwik_Period
     */
    public static function makePeriodFromQueryParams($site, $strPeriod, $strDate)
    {
        $tz = $site->getTimezone();

        if ($strPeriod == 'range') {
            $oPeriod = new Piwik_Period_Range('range', $strDate, $tz, Piwik_Date::factory('today', $tz));
        } else {
            $oDate = $strDate;
            if (!($strDate instanceof Piwik_Date)) {
                if ($strDate == 'now' || $strDate == 'today') {
                    $strDate = date('Y-m-d', Piwik_Date::factory('now', $tz)->getTimestamp());
                } elseif ($strDate == 'yesterday' || $strDate == 'yesterdaySameTime') {
                    $strDate = date('Y-m-d', Piwik_Date::factory('now', $tz)->subDay(1)->getTimestamp());
                }
                $oDate = Piwik_Date::factory($strDate);
            }
            $date = $oDate->toString();
            $oPeriod = Piwik_Period::factory($strPeriod, $oDate);
        }

        return $oPeriod;
    }
    
    /**
     * Returns the value of the element $name from the current archive 
     * The value to be returned is a numeric value and is stored in the archive_numeric_* tables
     *
     * @param string  $name  For example Referers_distinctKeywords
     * @return float|int|false  False if no value with the given name
     * TODO: modify
     */
    public function getNumeric( $names )
    {
        $rows = $this->get($names, 'archive_numeric');
        return $this->createSimpleGetResult($rows, $names, $createDataTable = false, $isSimpleTable = true, $isNumeric = true);
    }
    
    /**
     * Returns the value of the element $name from the current archive
     * 
     * The value to be returned is a blob value and is stored in the archive_numeric_* tables
     * 
     * It can return anything from strings, to serialized PHP arrays or PHP objects, etc.
     *
     * @param string  $name  For example Referers_distinctKeywords
     * @return mixed  False if no value with the given name
     * TODO: modify
     */
    public function getBlob( $names, $idSubTable = null )
    {
        $rows = $this->get($names, 'archive_blob', $idSubTable);
        return $this->createSimpleGetResult($rows, $names, $createDataTable = false);
    }
    
    /**
     *
     * @param $fields
     * @return Piwik_DataTable
     * TODO: modify
     */
    public function getDataTableFromNumeric( $names )
    {
        $rows = $this->get($names, 'archive_numeric');
        return $this->createSimpleGetResult($rows, $names, $createDataTable = true, $isSimpleTable = true, $isNumeric = true);
    }

    /**
     * This method will build a dataTable from the blob value $name in the current archive.
     * 
     * For example $name = 'Referers_searchEngineByKeyword' will return a  Piwik_DataTable containing all the keywords
     * If a idSubTable is given, the method will return the subTable of $name 
     * 
     * @param string $name
     * @param int $idSubTable or null if requesting the parent table
     * @return Piwik_DataTable
     * @throws exception If the value cannot be found
     * TODO: modify
     */
    public function getDataTable( $name, $idSubTable = null )
    {
        $rows = $this->getDataTableImpl($name, $idSubTable);
        return $this->createSimpleGetResult($rows, $name, $createDataTable = true, $isSimpleTable = false);
    }
    
    /**
     * TODO
     * TODO: only allows one name right now, but that could be changed. Would need an IndexedByName
     *       datatable array type & more modifications to createSimpleGetResult.
     */
    private function getDataTableImpl( $name, $idSubTable )
    {
        $rows = $this->get($name, 'archive_blob', $idSubTable);
        
        // deserialize each string blob value into a DataTable
        foreach ($rows as &$dates) {
            foreach ($dates as &$row) {
                $value = reset($row);
                
                $table = new Piwik_DataTable();
                if (!empty($value)) {
                    $table->addRowsFromSerializedArray($value);
                }
                
                $row = $table;
            }
        }
        
        return $rows;
    }
    
    /**
     * Same as getDataTable() except that it will also load in memory
     * all the subtables for the DataTable $name. 
     * You can then access the subtables by using the Piwik_DataTable_Manager getTable() 
     *
     * @param string    $name
     * @param int|null  $idSubTable  null if requesting the parent table
     * @return Piwik_DataTable
     * TODO: modify
     * TODO: add clearCache method.
     * TODO: should this have an idSubtable param? look if its ever called w/ it.
     * TODO: rename idSubTable to idSubtable.
     */
    public function getDataTableExpanded( $name, $idSubTable = null, $addMetadataSubtableId = true )
    {
        // cache all blobs of this type using one SQL request
        $this->getDataTableImpl($name, 'all');
        
        $recordName = $name;
        if ($idSubTable !== null) {
            $recordName .= '_' . $idSubTable;
        }
        
        // get top-level data
        $rows = array();
        foreach ($this->blobCache as $idSite => $dates) {
            foreach ($dates as $dateRange => $blobs) {
                $table = new Piwik_DataTable();
                if (!empty($blobs[$recordName])) {
                    $table->addRowsFromSerializedArray($blobs[$recordName]);
                }
                
                $rows[$idSite][$dateRange] = $table;
            }
        }
        
        // fetch subtables
        foreach ($rows as $idsite => $dates) {
            foreach ($dates as $dateRange => $table) {
                $tableMonth = $this->getTableMonthFromDateRange($dateRange);
                //$idarchive = $this->idarchives[$cacheKey][$idsite][$dateRange]; // TODO: need index by site/range now?
                $this->fetchSubTables($table, $name, $idsite, $dateRange, $addMetadataSubtableId);
                $table->enableRecursiveFilters();
            }
        }
        
        unset($this->blobCache);
        $this->blobCache = array();
        
        return $this->createSimpleGetResult($rows, $name, $createDataTable = true, $isSimpleTable = false);
    }
    
    /**
     * TODO
     * TODO: possible improvement. should be able to select several name types at a time. Right
     *       now, can only do one name at a time.
     * TODO: speed issue, should select ALL subtables like name_% in desired range and just look through them. at least this is how its done in Single.php.
     * does breadth first search
     */
    private function fetchSubTables( $table, $name, $idSite, $dateRange, $addMetadataSubtableId = true )
    {
        foreach ($table->getRows() as $row) {
            $sid = $row->getIdSubDataTable();
            if ($sid === null) {
                continue;
            }
            
            $blobName = $name."_".$sid;
            if (isset($this->blobCache[$idSite][$dateRange][$blobName])) {
                $blob = $this->blobCache[$idSite][$dateRange][$blobName];
            
                $subtable = new Piwik_DataTable();
                $subtable->addRowsFromSerializedArray($blob);
                $this->fetchSubTables($subtable, $name, $idSite, $dateRange, $addMetadataSubtableId);
                
                // we edit the subtable ID so that it matches the newly table created in memory
                // NB: we dont overwrite the datatableid in the case we are displaying the table expanded.
                if ($addMetadataSubtableId) {
                    // this will be written back to the column 'idsubdatatable' just before rendering,
                    // see Renderer/Php.php
                    $row->addMetadata('idsubdatatable_in_db', $row->getIdSubDataTable());
                }
                
                $row->setSubtable($subtable);
            }
        }
    }
    
    /**
     * TODO
     */
    private function checkBlobCache( &$result, $archiveNames )
    {
        foreach ($this->siteIds as $idSite) {
            foreach ($this->periods as $subperiod) {
                $dateStr = $subperiod->getRangeString();
                foreach ($archiveNames as $name) {
                    if (!isset($this->blobCache[$idSite][$dateStr][$name])) {
                        return false;
                    }
                    
                    $result[$idSite][$dateStr][$name] = $this->blobCache[$idSite][$dateStr][$name];
                }
            }
        }
        return true;
    }
    
    /**
     * TODO
     * $idSubTable can be 'all' to get all tables like X
     */
    private function get( $archiveNames, $archiveTableType, $idSubTable = null )
    {
        $this->idarchives = null; // TODO if subsequent requests go to the same plugin, the idarchives don't have to be reset
        
        $result = array();
        
        if (!is_array($archiveNames)) {
            $archiveNames = array($archiveNames);
        }
        
        // apply idSubTable
        if (!is_null($idSubTable) && $idSubTable != 'all') {
            foreach ($archiveNames as &$name) {
                $name .= "_$idSubTable";
            }
        }
        
        // check for cached blobs
        if ($archiveTableType == 'archive_blob') {
            $foundAll = $this->checkBlobCache($result, $archiveNames);
            if ($foundAll) {
                return $result;
            }
        }
        
        // get the archive IDs
        $archiveIds = $this->getArchiveIds($archiveNames);
        if (empty($archiveIds)) {
            return array();
        }
        
        // Creating the default array, to ensure consistent order
        $defaultValues = array();
        foreach ($archiveNames as $name) {
            $defaultValues[$name] = 0;
        }
        
        // create the SQL to select archive data
        $inNames = Piwik_Common::getSqlStringFieldsArray($archiveNames);
        if ($idSubTable != 'all') {
            $getValuesSql = "SELECT name, value, idsite, date1, date2
                               FROM %s
                              WHERE idarchive IN (%s)
                                AND name IN ($inNames)";
            $bind = array_values($archiveNames);
        } else {
            // select blobs w/ name like "$name_[0-9]+" w/o using RLIKE
            $nameEnd = strlen($name) + 2;
            $getValuesSql = "SELECT value, name, idsite, date1, date2
                                FROM %s
                                WHERE idarchive IN (%s)
                                  AND (name = ? OR
                                            (name LIKE ? AND SUBSTRING(name, $nameEnd, 1) >= '0'
                                                         AND SUBSTRING(name, $nameEnd, 1) <= '9') )";
            $bind = array($name, $name.'%');
        }
        
        // get data from every table we're querying
        foreach ($archiveIds as $tableMonth => $ids) {
            $table = Piwik_Common::prefixTable($archiveTableType."_".$tableMonth);
            $sql = sprintf($getValuesSql, $table, implode(',', $ids));
            
            foreach (Piwik_FetchAll($sql, $bind) as $row) {
                // values are grouped by idsite (site ID), date1-date2 (date range), then name (field name)
                $idSite = $row['idsite'];
                $periodStr = $row['date1'].",".$row['date2'];
                
                if (!isset($result[$idSite][$periodStr])) {
                    $result[$idSite][$periodStr] = $defaultValues;
                }
                
                if ($idSubTable != 'all') {
                    $value = $archiveTableType == 'archive_numeric'
                        ? (float)$row['value'] : $this->uncompress($row['value']);
                    $result[$idSite][$periodStr][$row['name']] = $value; // TODO: for getDataTableNumeric, this means one row per site/period. is this correct? Will this be the case for every get... method? Need to make this clear somewhere, piwik docs don't mention anything about this. 
                } else if ($archiveTableType == 'archive_blob') {
                    $value = $this->uncompress($row['value']);
                    
                    $result[$idSite][$periodStr][$row['name']] = $value;
                    $this->blobCache[$idSite][$periodStr][$row['name']] = $value;
                }
            }
        }
        
        return $result;
    }
    
    /**
     * TODO
     */
    private function getArchiveIds( $archiveNames )
    {
        $requestedReports = $this->getRequestedReports($archiveNames);
        
        if (!is_null($this->idarchives)) {
            // figure out which reports haven't been processed
            $cacheKeys = array();
            $reportsToArchive = array();
            foreach ($requestedReports as $report) {
                $cacheKey = $this->getArchiveCacheKey($report); // TODO: change name of getArchiveCacheKey
                
                $cacheKeys[$cacheKey] = true;
                if (!isset($this->idarchives[$cacheKey])) {
                    $reportsToArchive[] = $report;
                }
            }
            
            if (!empty($reportsToArchive)
                && !$this->isArchivingDisabled()
            ) {
                $this->getArchiveIdsAfterLaunching($reportsToArchive);
            }
            
            $idArchivesByMonth = array();
            foreach ($this->idarchives as $key => $sites) {
                // if this set of idarchives isn't related to the desired archive fields, don't use them
                if (!isset($cacheKeys[$key])) {
                    continue;
                }
                
                foreach ($sites as $idsite => $dates) {
                    foreach ($dates as $dateRange => $pair) {
                        list($isThereSomeVisits, $idarchive) = $pair;
                        if (!$isThereSomeVisits) continue;
                        
                        $tableMonth = $this->getTableMonthFromDateRange($dateRange);
                        $idArchivesByMonth[$tableMonth][] = $idarchive;
                    }
                }
            }
            
            return $idArchivesByMonth;
        }
        
        if (!$this->isArchivingDisabled()) {
            return $this->getArchiveIdsAfterLaunching($requestedReports);
        } else {
            return $this->getArchiveIdsWithoutLaunching($archiveNames);
        }
    }
    
    /**
     * TODO
 static $total = 0;static $count = 0;
$before = memory_get_usage();
        $delta = memory_get_usage() - $before;$total += $delta;++$count;
        if ($delta > 512) {
        echo "<pre>ARCHIVE IDS DELTA: ".Piwik::getPrettySizeFromBytes($delta)."\nTOTAL: ".Piwik::getPrettySizeFromBytes($total)."\nTOTAL COUNT: $count</pre>";}
     */
    private function getArchiveIdsAfterLaunching( $requestedReports )
    {
        $result = array();
        $today = Piwik_Date::today();
        
        // for every individual query permutation, launch the archiving process and get the archive ID
        foreach ($this->getPeriodsByTableMonth() as $tableMonth => $periods) {
            foreach ($this->siteIds as $idSite) {
                $site = new Piwik_Site($idSite);

                foreach ($periods as $period) {
                    $periodStr = $period->getRangeString();
                    
                    // if the END of the period is BEFORE the website creation date
                    // we already know there are no stats for this period
                    // we add one day to make sure we don't miss the day of the website creation
                    if ($period->getDateEnd()->addDay(2)->isEarlier($site->getCreationDate())) {
                    // TODO: log message (& below)
                        Piwik::log("TODO skipped, archive is before the website was created.");
                        continue;
                    }
            
                    // if the starting date is in the future we know there is no visit
                    if ($period->getDateStart()->subDay(2)->isLater($today)) {
                        // TODO: creates unecessary Piwik_Date instances (same as above if)
                        Piwik::log("TODO skipped, archive is after today.");
                        continue;
                    }
                    
                    // prepare the ArchiveProcessing instance
                    $processing = $this->getArchiveProcessingInstance($period);
                    $processing->setSite($site);
                    $processing->setPeriod($period);
                    $processing->setSegment($this->segment);
                    
                    $processing->isThereSomeVisits = null;
                    
                    // process for each requested report as well
                    foreach ($requestedReports as $report) {
                        $processing->init();
                        $processing->setRequestedReport($report);
                        
                        // launch archiving if the requested data hasn't been archived
                        $idArchive = $processing->loadArchive();
                        if (empty($idArchive)) {
                            $processing->launchArchiving();
                            $idArchive = $processing->getIdArchive();
                        }
                        
                        // store & cache the archive ID
                        $result[$tableMonth][] = $idArchive;
                        
                        $cacheKey = $this->getArchiveCacheKey($report);
                        $this->idarchives[$cacheKey][$idSite][$periodStr] =
                            array($processing->isThereSomeVisits, $idArchive);
                    }
                }
            }
        }
        
        return $result;
    }
    
    private $processingCache = array();
    
    /**
     * TODO
     */
    private function getArchiveProcessingInstance($period)
    {
        $label = $period->getLabel();
        if (!isset($this->processingCache[$label])) {
            $this->processingCache[$label] = Piwik_ArchiveProcessing::factory($label);
        }
        return $this->processingCache[$label];
    }
    
    /**
     * TODO
     */
    private function getArchiveIdsWithoutLaunching( $archiveNames )
    {
        $piwikTables = Piwik::getTablesInstalled(); // TODO: will this be too slow?
        
        $getArchiveIdsSql = "SELECT idsite, date1, date2, MAX(idarchive) as idarchive
                               FROM %s
                              WHERE period = ?
                                AND %s
                                AND ".$this->getNameCondition($archiveNames)."
                                AND idsite IN (".implode(',', $this->siteIds).")
                           GROUP BY idsite, date1, date2";
        
        // for every month within the archive query, select from numeric table
        $result = array();
        foreach ($this->getPeriodsByTableMonth() as $tableMonth => $subPeriods) {
            $firstPeriod = $subPeriods[0];
            $table = Piwik_Common::prefixTable("archive_numeric_$tableMonth");
            
            // if the table doesn't exist, there are no archive IDs
            if (!in_array($table, $piwikTables)) {
                continue;
            }
            
            // if looking for a range archive. NOTE: we assume there's only one period if its a range.
            $bind = array($firstPeriod->getId());
            if ($firstPeriod instanceof Piwik_Period_Range) {
                $dateCondition = "date1 = ? AND date2 = ?";
                $bind[] = $firstPeriod->getDateStart()->toString('Y-m-d');
                $bind[] = $firstPeriod->getDateEnd()->toString('Y-m-d');
            } else { // if looking for a normal period
                $dateStrs = array();
                foreach ($subPeriods as $period) {
                    $dateStrs[] = $period->getDateStart()->toString('Y-m-d');
                }
                
                $dateCondition = "date1 IN ('".implode("','", $dateStrs)."')";
            }
            
            $sql = sprintf($getArchiveIdsSql, $table, $dateCondition);
            
            // get the archive IDs
            $archiveIds = array();
            foreach (Piwik_FetchAll($sql, $bind) as $row) {
                $archiveIds[] = $row['idarchive'];
                
                $dateStr = $row['date1'].",".$row['date2'];
                $idSite = (int)$row['idsite'];
                $this->idarchives['all'][$idSite][$dateStr] = array(true, $row['idarchive']); // TODO: any way to get isThereSomeVisits w/ this optimization?
            }
            
            if (!empty($archiveIds)) {
                $result[$tableMonth] = $archiveIds;
            }
        }
        
        return $result;
    }
    
    /**
     * TODO
     */
    private function getNameCondition( $archiveNames )
    {
        // the flags used to tell how the archiving process for a specific archive was completed,
        // if it was completed
        $doneFlags = array();
        $periodType = $this->getPeriodLabel();
        foreach ($archiveNames as $name) {
            $done = Piwik_ArchiveProcessing::getDoneStringFlagFor($this->segment, $periodType, $name);
            $donePlugins = Piwik_ArchiveProcessing::getDoneStringFlagFor($this->segment, $periodType, $name, true);
            
            $doneFlags[$done] = $done;
            $doneFlags[$donePlugins] = $donePlugins;
        }

        $allDoneFlags = "'".implode("','", $doneFlags)."'";
        
        // create the SQL to find archives that are DONE
        return "(name IN ($allDoneFlags)) AND
                (value = '".Piwik_ArchiveProcessing::DONE_OK."' OR
                 value = '".Piwik_ArchiveProcessing::DONE_OK_TEMPORARY."')";
    }
    
    /**
     * TODO
     */
    private function getPeriodsByTableMonth()
    {
        $result = array();
        foreach ($this->periods as $period) {
            $tableMonth = $period->getDateStart()->toString('Y_m');
            $result[$tableMonth][] = $period;
        }
        return $result;
    }
    
    /**
     * TODO
     */
    private function getPeriodLabel()
    {
        return reset($this->periods)->getLabel();
    }
    
    /**
     * TODO
     */
    private function getTableMonthFromDateRange( $dateRange )
    {
        return str_replace('-', '_', substr($dateRange, 0, 7));
    }
    
    /**
     * TODO
     */
    public function getRequestedReports( $archiveNames )
    {
        $result = array();
        foreach ($archiveNames as $name) {
            $result[] = self::getRequestedReport($name);
        }
        return array_unique($result);
    }
    
    /**
     * TODO
     */
    public static function getRequestedReport( $archiveName )
    {
        // Core metrics are always processed in Core, for the requested date/period/segment
        if (in_array($archiveName, Piwik_ArchiveProcessing::getCoreMetrics())
            || $archiveName == 'max_actions'
        ) {
            return 'VisitsSummary_CoreMetrics';
        }
        // VisitFrequency metrics don't follow the same naming convention (HACK) 
        else if(strpos($archiveName, '_returning') > 0
            // ignore Goal_visitor_returning_1_1_nb_conversions 
            && strpos($archiveName, 'Goal_') === false
        ) {
            return 'VisitFrequency_Metrics';
        }
        // Goal_* metrics are processed by the Goals plugin (HACK)
        else if(strpos($archiveName, 'Goal_') === 0) {
            return 'Goals_Metrics';
        } else {
            return $archiveName;
        }
    }
    
    /**
     * TODO
     */
    private function getArchiveCacheKey( $nameOrReport )
    {
        $report = self::getRequestedReport($nameOrReport);
        
        // TODO: isArchivingDisabled gets called a lot. need to cache?
        if ($this->getPeriodLabel() == 'range') {
            return Piwik_ArchiveProcessing::getPluginBeingProcessed($report);
        } else {
            return 'all';
        }
    }
    
    /**
     * TODO
     */
    private function createSimpleGetResult( $rows, $names, $createDataTable, $isSimpleTable = true, $isNumeric = false )
    {
        if (!is_array($names)) {
            $names = array($names);
        }
        
        // add empty arrays for sites & dates that have no data
        foreach ($this->siteIds as $idSite) {
            if (!isset($rows[$idSite])) {
                $rows[$idSite] = array();
            }
            
            $byDate = &$rows[$idSite];
            foreach ($this->periods as $subperiod) {
                $dateStr = $subperiod->getRangeString();
                $prettyDate = $subperiod->getPrettyString();
                
                if (!isset($byDate[$dateStr])) {
                    $byDate[$prettyDate] = array();
                } else {
                    // replace date range string w/ pretty date string
                    $byDate[$prettyDate] = $byDate[$dateStr];
                    unset($byDate[$dateStr]);
                }
            }
        }
        
        // TODO: what about 'ts_archived' metadata? boy this is getting painful...
        // set result metadata
        $metadata = array();
        foreach ($this->siteIds as $idSite) {
            $oSite = new Piwik_Site($idSite);
            foreach ($this->periods as $period) {
                $prettyDate = $period->getPrettyString();
                $metadata[$idSite][$prettyDate] = array(
                    'timestamp' => $period->getDateStart()->getTimestamp(),
                    'site' => $oSite,
                    'period' => $period,
                );
            }
        }
        
        // simplify result
        if (count($this->siteIds) == 1
            && !$this->forceIndexedBySite
        ) {
            if (count($this->periods) == 1    // 1 site, 1 date
                && !$this->forceIndexedByDate
            ) {
                // return one row/value
                if (empty($rows)) {
                    return false;
                } else {
                    $firstValue = reset($rows);
                    
                    if (empty($firstValue)) {
                        return false;
                    } else {
                        $rows = reset($firstValue);
                        
                        // special case for one site, one date, one metric request (backwards compatibility)
                        if ($isNumeric
                            && empty($rows)
                        ) {
                            foreach ($names as $name) {
                                $rows[$name] = 0;
                            }
                        }
                        
                        $firstMetadata = reset($metadata);
                        $metadata = reset($firstMetadata);
                        $indices = array();
                    }
                }
            } else { // 1 site, multiple dates
                // remove top-level site index
                $rows = reset($rows);
                $metadata = reset($metadata);
                $indices = array('date');
            }
        } else {
            if (count($this->periods) == 1 && !$this->forceIndexedByDate) { // multiple sites, 1 date
                // remove inner date arrays
                foreach ($rows as $idSite => &$value) {
                    $value = reset($value);
                }
                foreach ($metadata as $key => &$value) {
                    $value = reset($value);
                }
                $indices = array('idSite');
            } else { // multiple sites, multiple dates
                $indices = array('idSite', 'date');
            }
        }
        
        // create datatable if desired
        if ($createDataTable) {
            if ($rows instanceof Piwik_DataTable) {
                $rows->metadata = $metadata;
                return $rows;
            }
            
            return Piwik_DataTable::createIndexedFromArray($rows, $metadata, $indices, $isSimpleTable);
        } else {
            if (empty($indices) && count($names) == 1) {
                return reset($rows);
            }
            return $rows;
        }
    }
    
    /**
     * Helper - Loads a DataTable from the Archive.
     * Optionally loads the table recursively,
     * or optionally fetches a given subtable with $idSubtable
     *
     * @param string $name
     * @param int $idSite
     * @param string $period
     * @param Piwik_Date $date
     * @param string $segment
     * @param bool $expanded
     * @param null $idSubtable
     * @return Piwik_DataTable|Piwik_DataTable_Array
     */
    public static function getDataTableFromArchive($name, $idSite, $period, $date, $segment, $expanded, $idSubtable = null)
    {
        Piwik::checkUserHasViewAccess($idSite);
        $archive = Piwik_Archive::build($idSite, $period, $date, $segment);
        if ($idSubtable === false) {
            $idSubtable = null;
        }

        if ($expanded) {
            $dataTable = $archive->getDataTableExpanded($name, $idSubtable);
        } else {
            $dataTable = $archive->getDataTable($name, $idSubtable);
        }

        $dataTable->queueFilter('ReplaceSummaryRowLabel');

        return $dataTable;
    }

    protected function formatNumericValue($value)
    {
        // If there is no dot, we return as is
        // Note: this could be an integer bigger than 32 bits
        if (strpos($value, '.') === false) {
            if ($value === false) {
                return 0;
            }
            return (float)$value;
        }

        // Round up the value with 2 decimals
        // we cast the result as float because returns false when no visitors
        $value = round((float)$value, 2);
        return $value;
    }

    /**
     * Returns true if Segmentation is allowed for this user
     *
     * @return bool
     */
    public static function isSegmentationEnabled()
    {
        return !Piwik::isUserIsAnonymous()
            || Piwik_Config::getInstance()->General['anonymous_user_enable_use_segments_API'];
    }

    /**
     * Indicate if $dateString and $period correspond to multiple periods
     *
     * @static
     * @param  $dateString
     * @param  $period
     * @return boolean
     */
    public static function isMultiplePeriod($dateString, $period)
    {
        return (preg_match('/^(last|previous){1}([0-9]*)$/D', $dateString, $regs)
            || Piwik_Period_Range::parseDateRange($dateString))
            && $period != 'range';
    }

    /**
     * Indicate if $idSiteString corresponds to multiple sites.
     *
     * @param string $idSiteString
     * @return bool
     */
    public static function isMultipleSites($idSiteString)
    {
        return $idSiteString == 'all' || strpos($idSiteString, ',') !== false;
    }
    
    /**
     * TODO
     */
    public function isArchivingDisabled()
    {
        return Piwik_ArchiveProcessing::isArchivingDisabledFor($this->segment, $this->getPeriodLabel());
    }
    
    private function uncompress( $data )
    {
        return @gzuncompress($data);
    }
    
    /**
     * TODO
     */
    public function getBlobCache()
    {
        return $this->blobCache;
    }
}
