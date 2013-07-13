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
 * @package Piwik_SegmentEditor
 */
class Piwik_SegmentEditor extends Piwik_Plugin
{
    public function getInformation()
    {
        return array(
            'description'     => 'Create and reuse custom visitor Segments with the Segment Editor.',
            'author'          => 'Piwik',
            'author_homepage' => 'http://piwik.org/',
            'version'         => Piwik_Version::VERSION,
        );
    }

    public function getListHooksRegistered()
    {
        return array(
            'Piwik.getKnownSegmentsToArchiveForSite'  => 'getKnownSegmentsToArchiveForSite',
            'Piwik.getKnownSegmentsToArchiveAllSites' => 'getKnownSegmentsToArchiveAllSites',
            'AssetManager.getJsFiles'                 => 'getJsFiles',
            'AssetManager.getCssFiles'                => 'getCssFiles',
            'template_nextToCalendar'                 => 'getSegmentEditorHtml',
        );
    }

    function getSegmentEditorHtml($notification)
    {
        $out =& $notification->getNotificationObject();
        $controller = new Piwik_SegmentEditor_Controller();
        $out .= $controller->getSelector();
    }

    public function getKnownSegmentsToArchiveAllSites($notification)
    {
        $segments =& $notification->getNotificationObject();
        $segmentToAutoArchive = Piwik_SegmentEditor_API::getInstance()->getSegmentsToAutoArchive();
        if (!empty($segmentToAutoArchive)) {
            $segments = array_merge($segments, $segmentToAutoArchive);
        }
    }

    public function getKnownSegmentsToArchiveForSite($notification)
    {
        $segments =& $notification->getNotificationObject();
        $idSite = $notification->getNotificationInfo();
        $segmentToAutoArchive = Piwik_SegmentEditor_API::getInstance()->getSegmentsToAutoArchive($idSite);

        foreach ($segmentToAutoArchive as $segmentInfo) {
            $segments[] = $segmentInfo['definition'];
        }
        $segments = array_unique($segments);
    }

    public function install()
    {
        $dao = Piwik_Db_Factory::getDao('segment');
        $dao->install();
    }

    public function getJsFiles($notification)
    {
        $jsFiles = & $notification->getNotificationObject();
        $jsFiles[] = "plugins/SegmentEditor/templates/jquery.jscrollpane.js";
        $jsFiles[] = "plugins/SegmentEditor/templates/Segmentation.js";
        $jsFiles[] = "plugins/SegmentEditor/templates/jquery.mousewheel.js";
        $jsFiles[] = "plugins/SegmentEditor/templates/mwheelIntent.js";
    }

    public function getCssFiles($notification)
    {
        $cssFiles = & $notification->getNotificationObject();
        $cssFiles[] = "plugins/SegmentEditor/templates/Segmentation.css";
        $cssFiles[] = "plugins/SegmentEditor/templates/jquery.jscrollpane.css";
        $cssFiles[] = "plugins/SegmentEditor/templates/scroll.css";
    }

}
