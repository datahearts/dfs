package dfs

import (
	log "datahearts.com/logging"
)

/**
 * Move files or folders
 * @param from the source folder inode
 * @param oldName the original file name
 * @param to the dest folder inode
 * @param newName the new file name
 * @return *DFS
 */
func dfsMove(from uint64, oldName string, to uint64, newName string) error {
	log.Tracef("Move from:%v, oldName:%v, to:%v, newName:%v", from, oldName, to, newName)

	//Get the source folder inode
	oldParent, err := GetAndLockINode(from)
	if err != nil {
		log.Errorf("Failed to move file %v. %v", oldName, err)
		return err
	}
	//Get the source file inode
	oldFile, err := oldParent.GetChildINode(oldName)
	if err != nil {
		UnlockINode(oldParent)
		log.Errorf("Failed to move file %v. %v", oldName, err)
		return err
	}

	var newParent *INode

	//Move in the same folder
	if from == to {
		newParent = oldParent
	} else {
		//Get the dest folder inode
		newParent, err = GetAndLockINode(to)
		if err != nil {
			UnlockINode(oldParent)
			log.Errorf("Failed to move file %v. %v", oldName, err)
			return err
		}
	}

	//Get the dest file inode
	newFile, err := newParent.GetChildINode(newName)
	if newFile != nil {
		log.Tracef("The target file is existing,remove it.")
		newFile.RemoveINode()
	}

	if from == to {
		//Change the file name
		err = oldParent.folderRename(oldFile, newName)
		if err != nil {
			UnlockINode(oldParent)
			log.Errorf("Failed to move file %v. %v", oldName, err)
			return err
		}
		newFile = oldFile
		newFile.FileName = newName
	} else {
		//Move between folders
		err = oldParent.FolderRemove(oldFile)
		if err != nil {
			UnlockINode(oldParent)
			UnlockINode(newParent)
			log.Errorf("Failed to move file %v. %v", oldName, err)
			return err
		}

		newFile = oldFile
		newFile.FileName = newName
		newFile.ParentId = to
		err = newParent.FolderAdd(newFile)
		if err != nil {
			UnlockINode(newParent)
			log.Errorf("Failed to move file %v. %v", oldName, err)
			return err
		}
	}

	if err == nil {
		//Save the new file inode
		err = newFile.SaveINode()
	}

	return err
}
