<?php
/**
 * Piwik - free/libre analytics platform
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 */
namespace Piwik\Plugins\SegmentEditor;

use Piwik\DbHelper;

/**
 * The SegmentEditor Model lets you persist and read custom Segments from the backend without handling any logic.
 */
class PgModel extends Model
{
    public static function install()
    {
        $segmentTable = "idsegment SERIAL4 NOT NULL,
					     name VARCHAR(255) NOT NULL,
					     definition TEXT NOT NULL,
					     login VARCHAR(100) NOT NULL,
					     enable_all_users SMALLINT NOT NULL DEFAULT 0,
					     enable_only_idsite INTEGER NULL,
					     auto_archive SMALLINT NOT NULL DEFAULT 0,
					     ts_created TIMESTAMP WITHOUT TIME ZONE NULL,
					     ts_last_edit TIMESTAMP WITHOUT TIME ZONE NULL,
					     deleted SMALLINT NOT NULL DEFAULT 0,
					     PRIMARY KEY (idsegment)";

        DbHelper::createTable(self::$rawPrefix, $segmentTable);
    }
}

