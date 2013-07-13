<?php
/**
 * Piwik - Open source web analytics
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 * @category Piwik_Plugins
 * @package Piwik_SegmentEditor
 */

/**
 * The SegmentEditor API lets you add, update, delete custom Segments, and list saved segments.
 *
 * @package Piwik_SegmentEditor
 */
class Piwik_SegmentEditor_API
{
    static private $instance = null;

    /**
     * @return Piwik_SegmentEditor_API
     */
    static public function getInstance()
    {
        if (self::$instance == null) {
            self::$instance = new self;
        }
        return self::$instance;
    }

    protected function checkSegmentValue($definition, $idSite)
    {
        try {
            $segment = new Piwik_Segment($definition, $idSite);
            $segment->getHash();
        } catch (Exception $e) {
            throw new Exception("The specified segment is invalid: " . $e->getMessage());
        }
    }

    protected function checkSegmentName($name)
    {
        if (empty($name)) {
            throw new Exception("Invalid name for this custom segment.");
        }
    }

    protected function checkEnabledAllUsers($enabledAllUsers)
    {
        $enabledAllUsers = (int)$enabledAllUsers;
        if ($enabledAllUsers
            && !Piwik::isUserIsSuperUser()
        ) {
            throw new Exception("&enabledAllUsers=1 requires Super User access");
        }
        return $enabledAllUsers;
    }


    /**
     * @param $idSite
     * @throws Exception
     */
    protected function checkIdSite($idSite)
    {
        if (empty($idSite)) {
            if (!Piwik::isUserIsSuperUser()) {
                throw new Exception("idSite is required, unless you are Super User and can create the segment across all websites");
            }
        } else {
            if (!is_numeric($idSite)) {
                throw new Exception("idSite should be a numeric value");
            }
            Piwik::checkUserHasViewAccess($idSite);
        }
    }

    protected function checkAutoArchive($autoArchive, $idSite)
    {
        $autoArchive = (int)$autoArchive;
        if ($autoArchive) {
            $exception = new Exception("To prevent abuse, autoArchive=1 requires Super User or Admin access.");
            if (empty($idSite)) {
                if (!Piwik::isUserIsSuperUser()) {
                    throw $exception;
                }
            } else {
                if (!Piwik::isUserHasAdminAccess($idSite)) {
                    throw $exception;
                }
            }
        }
        return $autoArchive;
    }

    public function delete($idSegment)
    {
        $segment = $this->getSegmentOrFail($idSegment);
        $dao = Piwik_Db_Factory::getDao('segment');
        $dao->deleteByIdsegment($idSegment);
        return true;
    }

    public function update($idSegment, $name, $definition, $idSite = false, $autoArchive = false, $enabledAllUsers = false)
    {
        $this->checkIdSite($idSite);
        $this->checkSegmentName($name);
        $this->checkSegmentValue($definition, $idSite);
        $enabledAllUsers = $this->checkEnabledAllUsers($enabledAllUsers);
        $autoArchive = $this->checkAutoArchive($autoArchive, $idSite);

        $segment = $this->getSegmentOrFail($idSegment);
        $dao = Piwik_Db_Factory::getDao('segment');
        $bind = array(
            'name'               => $name,
            'definition'         => $definition,
            'enable_all_users'   => $enabledAllUsers,
            'enable_only_idsite' => $idSite,
            'auto_archive'       => $autoArchive,
            'ts_last_edit'       => Piwik_Date::now()->getDateTime()
        );
        $dao->updateByIdsegment($bind, $idSegment);
        return true;
    }


    public function add($name, $definition, $idSite = false, $autoArchive = false, $enabledAllUsers = false)
    {
        Piwik::checkUserIsNotAnonymous();
        $this->checkIdSite($idSite);
        $this->checkSegmentName($name);
        $this->checkSegmentValue($definition, $idSite);
        $enabledAllUsers = $this->checkEnabledAllUsers($enabledAllUsers);
        $autoArchive = $this->checkAutoArchive($autoArchive, $idSite);

        $dao = Piwik_Db_Factory::getDao('segment');
        $bind = array(
            'name'               => $name,
            'definition'         => $definition,
            'login'              => Piwik::getCurrentUserLogin(),
            'enable_all_users'   => (int)$enabledAllUsers,
            'enable_only_idsite' => (int)$idSite,
            'auto_archive'       => (int)$autoArchive,
            'ts_created'         => Piwik_Date::now()->getDatetime(),
            'deleted'            => 0,
        );
        $id = $dao->add($bind);
        return $id;
    }

    public function getSegmentsToAutoArchive($idSite = false)
    {
        Piwik::checkUserIsSuperUser();
        $dao = Piwik_Db_Factory::getDao('segment');
        $segments = $dao->getSegmentsToAutoArchive($idSite);
        return $segments;
    }

    public function get($idSegment)
    {
        Piwik::checkUserHasSomeViewAccess();
        if (!is_numeric($idSegment)) {
            throw new Exception("idSegment should be numeric.");
        }
        $dao = Piwik_Db_Factory::getDao('segment');
        $segment = $dao->getByIdsegment($idSegment);

        if (empty($segment)) {
            return false;
        }
        try {
            Piwik::checkUserIsSuperUserOrTheUser($segment['login']);
        } catch (Exception $e) {
            throw new Exception("You can only manage your own segments (unless you are Super User).");
        }

        if ($segment['deleted']) {
            throw new Exception("This segment is marked as deleted.");
        }
        return $segment;
    }

    /**
     * @param $idSegment
     * @throws Exception
     */
    protected function getSegmentOrFail($idSegment)
    {
        $segment = $this->get($idSegment);

        if (empty($segment)) {
            throw new Exception("Requested segment not found");
        }
        return $segment;
    }

    public function getAll($idSite)
    {
        Piwik::checkUserHasViewAccess($idSite);

        $dao = Piwik_Db_Factory::getDao('segment');
        $segments = $dao->getAll($idSite, Piwik::getCurrentUserLogin());

        return $segments;
    }
}
