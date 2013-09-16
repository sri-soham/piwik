<?php
/**
 * Piwik - Open source web analytics
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 */
use Piwik\Access;
use Piwik\Plugins\MobileMessaging\API as MobileMessagingAPI;
use Piwik\Plugins\MobileMessaging\MobileMessaging;
use Piwik\Plugins\PDFReports\API as PDFReportsAPI;
use Piwik\Plugins\PDFReports\PDFReports;
use Piwik\Plugins\SitesManager\API as SitesManagerAPI;
use Piwik\ScheduledTime;
use Piwik\ScheduledTask;
use Piwik\ScheduledTime\Daily;
use Piwik\ScheduledTime\Monthly;
use Piwik\Site;

require_once 'PDFReports/PDFReports.php';

class PDFReportsTest extends DatabaseTestCase
{
    private $idSite = 1;

    public function setUp()
    {
        parent::setUp();

        // setup the access layer
        self::setSuperUser();
        \Piwik\PluginsManager::getInstance()->loadPlugins(array('API', 'UserCountry', 'PDFReports', 'MobileMessaging'));
        \Piwik\PluginsManager::getInstance()->installLoadedPlugins();

        SitesManagerAPI::getInstance()->addSite("Test", array("http://piwik.net"));

        SitesManagerAPI::getInstance()->addSite("Test", array("http://piwik.net"));
        FakeAccess::setIdSitesView(array($this->idSite, 2));
        PDFReportsAPI::$cache = array();
    }

    public function tearDown()
    {
        $Report = \Piwik\Db\Factory::getDAO('report');
        $Report->dropTable();
        parent::tearDown();
    }

    /**
     * @group Plugins
     * @group PDFReports
     */
    public function testAddReportGetReports()
    {
        $data = array(
            'idsite'      => $this->idSite,
            'description' => 'test description"',
            'type'        => 'email',
            'period'      => ScheduledTime::PERIOD_DAY,
            'hour'        => '4',
            'format'      => 'pdf',
            'reports'     => array('UserCountry_getCountry'),
            'parameters'  => array(
                'displayFormat'    => '1',
                'emailMe'          => true,
                'additionalEmails' => array('test@test.com', 't2@test.com'),
                'evolutionGraph'   => true
            )
        );

        $dataWebsiteTwo = $data;
        $dataWebsiteTwo['idsite'] = 2;
        $dataWebsiteTwo['period'] = ScheduledTime::PERIOD_MONTH;

        self::addReport($dataWebsiteTwo);

        // Testing getReports without parameters
        $tmp = PDFReportsAPI::getInstance()->getReports();
        $report = reset($tmp);
        $this->assertReportsEqual($report, $dataWebsiteTwo);

        $idReport = self::addReport($data);

        // Passing 3 parameters
        $tmp = PDFReportsAPI::getInstance()->getReports($this->idSite, $data['period'], $idReport);
        $report = reset($tmp);
        $this->assertReportsEqual($report, $data);

        // Passing only idsite
        $tmp = PDFReportsAPI::getInstance()->getReports($this->idSite);
        $report = reset($tmp);
        $this->assertReportsEqual($report, $data);

        // Passing only period
        $tmp = PDFReportsAPI::getInstance()->getReports($idSite = false, $data['period']);
        $report = reset($tmp);
        $this->assertReportsEqual($report, $data);

        // Passing only idreport
        $tmp = PDFReportsAPI::getInstance()->getReports($idSite = false, $period = false, $idReport);
        $report = reset($tmp);
        $this->assertReportsEqual($report, $data);
    }

    /**
     * @group Plugins
     * @group PDFReports
     */
    public function testGetReportsIdReportNotFound()
    {
        try {
            PDFReportsAPI::getInstance()->getReports($idSite = false, $period = false, $idReport = 1);
        } catch (Exception $e) {
            return;
        }
        $this->fail('Expected exception not raised');
    }

    /**
     * @group Plugins
     * @group PDFReports
     */
    public function testGetReportsInvalidPermission()
    {
        try {
            PDFReportsAPI::getInstance()->getReports(
                $idSite = 44,
                $period = false,
                self::addReport(self::getDailyPDFReportData($this->idSite))
            );

        } catch (Exception $e) {
            return;
        }
        $this->fail('Expected exception not raised');
    }

    /**
     * @group Plugins
     * @group PDFReports
     */
    public function testAddReportInvalidWebsite()
    {
        try {
            self::addReport(self::getDailyPDFReportData(33));
        } catch (Exception $e) {
            return;
        }
        $this->fail('Expected exception not raised');
    }

    /**
     * @group Plugins
     * @group PDFReports
     */
    public function testAddReportInvalidPeriod()
    {
        try {
            $data = self::getDailyPDFReportData($this->idSite);
            $data['period'] = 'dx';
            self::addReport($data);
        } catch (Exception $e) {
            return;
        }
        $this->fail('Expected exception not raised');
    }

    /**
     * @group Plugins
     * @group PDFReports
     */
    public function testUpdateReport()
    {
        $idReport = self::addReport(self::getDailyPDFReportData($this->idSite));
        $dataAfter = self::getMonthlyEmailReportData($this->idSite);

        self::updateReport($idReport, $dataAfter);

        $reports = PDFReportsAPI::getInstance()->getReports($idSite = false, $period = false, $idReport);

        $this->assertReportsEqual(
            reset($reports),
            $dataAfter
        );
    }

    /**
     * @group Plugins
     * @group PDFReports
     */
    public function testDeleteReport()
    {
        // Deletes non existing report throws exception
        try {
            PDFReportsAPI::getInstance()->deleteReport($idReport = 1);
            $this->fail('Exception not raised');
        } catch (Exception $e) {
        }

        $idReport = self::addReport(self::getMonthlyEmailReportData($this->idSite));
        $this->assertEquals(1, count(PDFReportsAPI::getInstance()->getReports()));
        PDFReportsAPI::getInstance()->deleteReport($idReport);
        $this->assertEquals(0, count(PDFReportsAPI::getInstance()->getReports()));
    }

    /**
     * @group Plugins
     * @group PDFReports
     */
    public function testGetTopMenuTranslationKeyMobileMessagingInactive()
    {
        // unload MobileMessaging plugin
        \Piwik\PluginsManager::getInstance()->loadPlugins(array('PDFReports'));

        $pdfReportPlugin = new PDFReports();
        $this->assertEquals(
            PDFReports::PDF_REPORTS_TOP_MENU_TRANSLATION_KEY,
            $pdfReportPlugin->getTopMenuTranslationKey()
        );
    }

    /**
     * @group Plugins
     * @group PDFReports
     */
    public function testGetTopMenuTranslationKeyUserIsAnonymous()
    {
        $anonymousAccess = new FakeAccess;
        FakeAccess::$identity = 'anonymous';
        Access::setSingletonInstance($anonymousAccess);

        $pdfReportPlugin = new PDFReports();
        $this->assertEquals(
            PDFReports::MOBILE_MESSAGING_TOP_MENU_TRANSLATION_KEY,
            $pdfReportPlugin->getTopMenuTranslationKey()
        );
    }

    /**
     * top menu should display 'Email & SMS reports' when the user has set-up a valid mobile provider account
     * even though there is no sms reports configured
     *
     * @group Plugins
     * @group PDFReports
     */
    public function testGetTopMenuTranslationKeyNoReportMobileAccountOK()
    {
        // set mobile provider account
        self::setSuperUser();
        MobileMessagingAPI::getInstance()->setSMSAPICredential('StubbedProvider', '');

        $pdfReportPlugin = new PDFReports();
        $this->assertEquals(
            PDFReports::MOBILE_MESSAGING_TOP_MENU_TRANSLATION_KEY,
            $pdfReportPlugin->getTopMenuTranslationKey()
        );
    }

    /**
     * top menu should display 'Email reports' when the user has not set-up a valid mobile provider account
     * and no reports at all have been configured
     *
     * @group Plugins
     * @group PDFReports
     */
    public function testGetTopMenuTranslationKeyNoReportMobileAccountKO()
    {
        $pdfReportPlugin = new PDFReports();
        $this->assertEquals(
            PDFReports::PDF_REPORTS_TOP_MENU_TRANSLATION_KEY,
            $pdfReportPlugin->getTopMenuTranslationKey()
        );
    }

    /**
     * top menu should display 'Email & SMS reports' if there is at least one sms report
     * whatever the status of the mobile provider account
     *
     * @group Plugins
     * @group PDFReports
     */
    public function testGetTopMenuTranslationKeyOneSMSReportMobileAccountKO()
    {
        PDFReportsAPI::getInstance()->addReport(
            1,
            '',
            ScheduledTime::PERIOD_DAY,
            0,
            MobileMessaging::MOBILE_TYPE,
            MobileMessaging::SMS_FORMAT,
            array(),
            array(
                 MobileMessaging::PHONE_NUMBERS_PARAMETER => array()
            )
        );

        $pdfReportPlugin = new PDFReports();
        $this->assertEquals(
            PDFReports::MOBILE_MESSAGING_TOP_MENU_TRANSLATION_KEY,
            $pdfReportPlugin->getTopMenuTranslationKey()
        );
    }

    /**
     * top menu should display 'Email reports' if there are no SMS reports and at least one email report
     * whatever the status of the mobile provider account
     *
     * @group Plugins
     * @group PDFReports
     */
    public function testGetTopMenuTranslationKeyNoSMSReportAccountOK()
    {
        // set mobile provider account
        self::setSuperUser();
        MobileMessagingAPI::getInstance()->setSMSAPICredential('StubbedProvider', '');

        self::addReport(self::getMonthlyEmailReportData($this->idSite));

        $pdfReportPlugin = new PDFReports();
        $this->assertEquals(
            PDFReports::PDF_REPORTS_TOP_MENU_TRANSLATION_KEY,
            $pdfReportPlugin->getTopMenuTranslationKey()
        );
    }

    /**
     * @group Plugins
     * @group PDFReports
     */
    public function testGetScheduledTasks()
    {
        // stub API to control getReports() return values
        $report1 = self::getDailyPDFReportData($this->idSite);
        $report1['idreport'] = 1;
        $report1['hour'] = 0;
        $report1['deleted'] = 0;

        $report2 = self::getMonthlyEmailReportData($this->idSite);
        $report2['idreport'] = 2;
        $report2['idsite'] = 2;
        $report2['hour'] = 0;
        $report2['deleted'] = 0;

        $report3 = self::getMonthlyEmailReportData($this->idSite);
        $report3['idreport'] = 3;
        $report3['deleted'] = 1; // should not be scheduled

        $report4 = self::getMonthlyEmailReportData($this->idSite);
        $report4['idreport'] = 4;
        $report4['idsite'] = 1;
        $report4['hour'] = 8;
        $report4['deleted'] = 0;

        $report5 = self::getMonthlyEmailReportData($this->idSite);
        $report5['idreport'] = 5;
        $report5['idsite'] = 2;
        $report5['hour'] = 8;
        $report5['deleted'] = 0;

        // test no exception is raised when a scheduled report is set to never send
        $report6 = self::getMonthlyEmailReportData($this->idSite);
        $report6['idreport'] = 6;
        $report6['period'] = ScheduledTime::PERIOD_NEVER;
        $report6['deleted'] = 0;

        $stubbedPDFReportsAPI = $this->getMock('\\Piwik\\Plugins\\PDFReports\\API');
        $stubbedPDFReportsAPI->expects($this->any())->method('getReports')->will($this->returnValue(
                array($report1, $report2, $report3, $report4, $report5, $report6))
        );

        $stubbedPDFReportsAPIClass = new ReflectionProperty('\\Piwik\\Plugins\\PDFReports\\API', 'instance');
        $stubbedPDFReportsAPIClass->setAccessible(true);
        $stubbedPDFReportsAPIClass->setValue($stubbedPDFReportsAPI);

        // initialize sites 1 and 2
        Site::$infoSites = array(
            1 => array('timezone' => 'Europe/Paris'),
            2 => array('timezone' => 'UTC-6.5'),
        );

        // expected tasks
        $scheduleTask1 = new Daily();
        $scheduleTask1->setHour(23); // paris is UTC-1, period ends at 23h UTC

        $scheduleTask2 = new Monthly();
        $scheduleTask2->setHour(7); // site is UTC-6.5, period ends at 6h30 UTC, smallest resolution is hour

        $scheduleTask3 = new Monthly();
        $scheduleTask3->setHour(7); // paris is UTC-1, configured to be sent at 8h

        $scheduleTask4 = new Monthly();
        $scheduleTask4->setHour(15); // site is UTC-6.5, configured to be sent at 8h

        $expectedTasks = array(
            new ScheduledTask (PDFReportsAPI::getInstance(), 'sendReport', 1, $scheduleTask1),
            new ScheduledTask (PDFReportsAPI::getInstance(), 'sendReport', 2, $scheduleTask2),
            new ScheduledTask (PDFReportsAPI::getInstance(), 'sendReport', 4, $scheduleTask3),
            new ScheduledTask (PDFReportsAPI::getInstance(), 'sendReport', 5, $scheduleTask4),
        );

        $pdfReportPlugin = new PDFReports();
        $tasks = array();
        $pdfReportPlugin->getScheduledTasks($tasks);
        $this->assertEquals($expectedTasks, $tasks);

        // restore API
        $stubbedPDFReportsAPIClass->setValue(null);
    }

    /**
     * Dataprovider for testGetReportSubjectAndReportTitle
     */
    public function getGetReportSubjectAndReportTitleTestCases()
    {
        return array(
            array('Piwik.org', 'General_Website Piwik.org', 'Piwik.org', array('UserSettings_getBrowserType')),
            array('Piwik.org', 'General_Website Piwik.org', 'Piwik.org', array('MultiSites_getAll', 'UserSettings_getBrowserType')),
            array('General_MultiSitesSummary', 'General_MultiSitesSummary', 'Piwik.org', array('MultiSites_getAll')),
        );
    }

    /**
     * @group Plugins
     * @group PDFReports
     * @dataProvider getGetReportSubjectAndReportTitleTestCases
     */
    public function testGetReportSubjectAndReportTitle($expectedReportSubject, $expectedReportTitle, $websiteName, $reports)
    {
        $getReportSubjectAndReportTitle = new ReflectionMethod(
            '\\Piwik\\Plugins\\PDFReports\\API', 'getReportSubjectAndReportTitle'
        );
        $getReportSubjectAndReportTitle->setAccessible(true);

        list($reportSubject, $reportTitle) = $getReportSubjectAndReportTitle->invoke(new PDFReportsAPI(), $websiteName, $reports);
        $this->assertEquals($expectedReportSubject, $reportSubject);
        $this->assertEquals($expectedReportTitle, $reportTitle);
    }

    private function assertReportsEqual($report, $data)
    {
        foreach ($data as $key => $value) {
            if ($key == 'description') $value = substr($value, 0, 250);
            $this->assertEquals($value, $report[$key], "Error for $key for report " . var_export($report, true) . " and data " . var_export($data, true));
        }
    }

    private static function addReport($data)
    {
        $idReport = PDFReportsAPI::getInstance()->addReport(
            $data['idsite'],
            $data['description'],
            $data['period'],
            $data['hour'],
            $data['type'],
            $data['format'],
            $data['reports'],
            $data['parameters']
        );
        return $idReport;
    }

    private static function getDailyPDFReportData($idSite)
    {
        return array(
            'idsite'      => $idSite,
            'description' => 'test description"',
            'period'      => ScheduledTime::PERIOD_DAY,
            'hour'        => '7',
            'type'        => 'email',
            'format'      => 'pdf',
            'reports'     => array('UserCountry_getCountry'),
            'parameters'  => array(
                'displayFormat'    => '1',
                'emailMe'          => true,
                'additionalEmails' => array('test@test.com', 't2@test.com'),
                'evolutionGraph'   => false
            )
        );
    }

    private static function getMonthlyEmailReportData($idSite)
    {
        return array(
            'idsite'      => $idSite,
            'description' => 'very very long and possibly truncated description. very very long and possibly truncated description. very very long and possibly truncated description. very very long and possibly truncated description. very very long and possibly truncated description. ',
            'period'      => ScheduledTime::PERIOD_MONTH,
            'hour'        => '0',
            'type'        => 'email',
            'format'      => 'pdf',
            'reports'     => array('UserCountry_getContinent'),
            'parameters'  => array(
                'displayFormat'    => '1',
                'emailMe'          => false,
                'additionalEmails' => array('blabla@ec.fr'),
                'evolutionGraph'   => false
            )
        );
    }

    private static function updateReport($idReport, $data)
    {
        $idReport = PDFReportsAPI::getInstance()->updateReport(
            $idReport,
            $data['idsite'],
            $data['description'],
            $data['period'],
            $data['hour'],
            $data['type'],
            $data['format'],
            $data['reports'],
            $data['parameters']);
        return $idReport;
    }

    private static function setSuperUser()
    {
        $pseudoMockAccess = new FakeAccess;
        FakeAccess::$superUser = true;
        Access::setSingletonInstance($pseudoMockAccess);
    }
}
