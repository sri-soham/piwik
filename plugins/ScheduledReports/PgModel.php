<?php
/**
 * Piwik - free/libre analytics platform
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 */
namespace Piwik\Plugins\ScheduledReports;

use Piwik\DbHelper;

class PgModel extends Model
{
    public static function install()
    {
        $reportTable = "idreport SERIAL4 NOT NULL,
					    idsite INTEGER NOT NULL,
					    login VARCHAR(100) NOT NULL,
					    description VARCHAR(255) NOT NULL,
					    idsegment INTEGER,
					    period VARCHAR(10) NOT NULL,
					    hour TINYINT NOT NULL DEFAULT 0,
					    type VARCHAR(10) NOT NULL,
					    format VARCHAR(10) NOT NULL,
					    reports TEXT NOT NULL,
					    parameters TEXT NULL,
					    ts_created TIMESTAMP WITHOUT TIME ZONE NULL,
					    ts_last_sent TIMESTAMP WITHOUT TIME ZONE NULL,
					    deleted TINYINT NOT NULL DEFAULT 0,
					    PRIMARY KEY (idreport)";

        DbHelper::createTable(self::$rawPrefix, $reportTable);
    }
}

