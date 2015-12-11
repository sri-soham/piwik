<?php
/**
 *  Piwik _ Open source web analytics
 *
 *  @link http://piwik.org
 *  @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 *  @category Piwik
 *  @package Piwik
 */
namespace Piwik\Db\DAO\Mysql;

use Piwik\Db\DAO\Base;

/**
 * Used for generating auto increment ids.
 *
 * Example:
 *
 * $sequence = new Sequence();
 * $sequence->setName('my_sequence_name');
 * $id = $sequence->getNextId();
 * $db->insert('anytable', array('id' => $id, '...' => '...'));
 */
class Sequence extends Base {
    /**
     * @var string
     */
    private $name;

    public function __construct($db, $table)
    {
        parent::__construct($db, $table);
    }

    /**
     * The name of the table or sequence you want to get an id for.
     *
     * @param string $name eg 'archive_numeric_2014_11'
     */
    public function setName($name)
    {
        $this->name = $name;
    }

    /**
     * Creates / initializes a new sequence.
     *
     * @param int $initialValue
     * @return int The actually used value to initialize the table.
     *
     * @throws \Exception in case a sequence having this name already exists.
     */
    public function create($initialValue = 0)
    {
        $initialValue = (int) $initialValue;

        $this->db->insert($this->table, array('name' => $this->name, 'value' => $initialValue));

        return $initialValue;
    }

    /**
     * Returns true if the sequence exist.
     *
     * @return bool
     */
    public function exists()
    {
        $query = $this->db->query('SELECT * FROM ' . $this->table . ' WHERE name = ?', $this->name);

        return $query->rowCount() > 0;
    }

    /**
     * Get / allocate / reserve a new id for the current sequence. Important: Getting the next id will fail in case
     * no such sequence exists. Make sure to create one if needed, see {@link create()}.
     *
     * @return int
     * @throws Exception
     */
    public function getNextId()
    {
        $sql   = 'UPDATE ' . $this->table . ' SET value = LAST_INSERT_ID(value + 1) WHERE name = ?';

        $result   = $this->db->query($sql, array($this->name));
        $rowCount = $result->rowCount();

        if (1 !== $rowCount) {
            throw new \Exception("Sequence '" . $this->name . "' not found.");
        }

        $createdId = $this->db->lastInsertId();

        return (int) $createdId;
    }

    /**
     * Returns the current max id.
     * @return int
     * @internal
     */
    public function getCurrentId()
    {
        $sql   = 'SELECT value FROM ' . $this->table . ' WHERE name = ?';

        $id = $this->db->fetchOne($sql, array($this->name));

        if (!empty($id) || '0' === $id || 0 === $id) {
            return (int) $id;
        }
    }

}

