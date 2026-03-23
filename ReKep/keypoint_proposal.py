import numpy as np
import torch
import cv2
import os
from torch.nn.functional import interpolate
from kmeans_pytorch import kmeans
from utils import filter_points_by_bounds
from sklearn.cluster import MeanShift

class KeypointProposer:
    def __init__(self, config):
        self.config = config
        self.device = torch.device(self.config['device'])
        self.dinov2 = torch.hub.load('facebookresearch/dinov2', 'dinov2_vits14').eval().to(self.device)
        self.bounds_min = np.array(self.config['bounds_min'])
        self.bounds_max = np.array(self.config['bounds_max'])
        self.mean_shift = MeanShift(
            bandwidth=self.config['min_dist_bt_keypoints'],
            bin_seeding=True,
            n_jobs=self._resolve_meanshift_n_jobs(),
        )
        self.patch_size = 14  # dinov2
        np.random.seed(self.config['seed'])
        torch.manual_seed(self.config['seed'])
        torch.cuda.manual_seed(self.config['seed'])

    def _resolve_meanshift_n_jobs(self):
        raw = os.environ.get("REKEP_MEANSHIFT_N_JOBS", "1")
        try:
            value = int(str(raw).strip())
        except Exception:
            return 1
        if value == 0:
            return 1
        if value < -1:
            return -1
        if value > 0:
            cpu_count = os.cpu_count() or 1
            return max(1, min(value, cpu_count))
        return value

    def get_keypoints(self, rgb, points, masks, return_metadata=False):
        # preprocessing
        transformed_rgb, rgb, points, masks, shape_info = self._preprocess(rgb, points, masks)
        # get features
        features_flat = self._get_features(transformed_rgb, shape_info)
        # for each mask, cluster in feature space to get meaningful regions, and uske their centers as keypoint candidates
        candidate_keypoints, candidate_pixels, candidate_rigid_group_ids = self._cluster_features(points, features_flat, masks)
        if candidate_keypoints.shape[0] == 0:
            if not return_metadata:
                return candidate_keypoints, rgb.copy()
            metadata = {
                'candidate_pixels': candidate_pixels,
                'candidate_rigid_group_ids': candidate_rigid_group_ids,
            }
            return candidate_keypoints, rgb.copy(), metadata
        # exclude keypoints that are outside of the workspace
        within_space = filter_points_by_bounds(candidate_keypoints, self.bounds_min, self.bounds_max, strict=True)
        candidate_keypoints = candidate_keypoints[within_space]
        candidate_pixels = candidate_pixels[within_space]
        candidate_rigid_group_ids = candidate_rigid_group_ids[within_space]
        if candidate_keypoints.shape[0] == 0:
            if not return_metadata:
                return candidate_keypoints, rgb.copy()
            metadata = {
                'candidate_pixels': candidate_pixels,
                'candidate_rigid_group_ids': candidate_rigid_group_ids,
            }
            return candidate_keypoints, rgb.copy(), metadata
        # merge close points by clustering in cartesian space
        merged_indices = self._merge_clusters(candidate_keypoints)
        candidate_keypoints = candidate_keypoints[merged_indices]
        candidate_pixels = candidate_pixels[merged_indices]
        candidate_rigid_group_ids = candidate_rigid_group_ids[merged_indices]
        # sort candidates by locations
        sort_idx = np.lexsort((candidate_pixels[:, 0], candidate_pixels[:, 1]))
        candidate_keypoints = candidate_keypoints[sort_idx]
        candidate_pixels = candidate_pixels[sort_idx]
        candidate_rigid_group_ids = candidate_rigid_group_ids[sort_idx]
        # project keypoints to image space
        projected = self._project_keypoints_to_img(rgb, candidate_pixels, candidate_rigid_group_ids, masks, features_flat)
        if not return_metadata:
            return candidate_keypoints, projected
        metadata = {
            'candidate_pixels': candidate_pixels,
            'candidate_rigid_group_ids': candidate_rigid_group_ids,
        }
        return candidate_keypoints, projected, metadata

    def _preprocess(self, rgb, points, masks):
        # convert masks to binary masks
        masks = [masks == uid for uid in np.unique(masks) if int(uid) > 0]
        # ensure input shape is compatible with dinov2
        H, W, _ = rgb.shape
        patch_h = int(H // self.patch_size)
        patch_w = int(W // self.patch_size)
        new_H = patch_h * self.patch_size
        new_W = patch_w * self.patch_size
        transformed_rgb = cv2.resize(rgb, (new_W, new_H))
        transformed_rgb = transformed_rgb.astype(np.float32) / 255.0  # float32 [H, W, 3]
        # shape info
        shape_info = {
            'img_h': H,
            'img_w': W,
            'patch_h': patch_h,
            'patch_w': patch_w,
        }
        return transformed_rgb, rgb, points, masks, shape_info
    
    def _project_keypoints_to_img(self, rgb, candidate_pixels, candidate_rigid_group_ids, masks, features_flat):
        projected = rgb.copy()
        # overlay keypoints on the image
        for keypoint_count, pixel in enumerate(candidate_pixels):
            displayed_text = f"{keypoint_count}"
            text_length = len(displayed_text)
            # draw a box
            box_width = 30 + 10 * (text_length - 1)
            box_height = 30
            cv2.rectangle(projected, (pixel[1] - box_width // 2, pixel[0] - box_height // 2), (pixel[1] + box_width // 2, pixel[0] + box_height // 2), (255, 255, 255), -1)
            cv2.rectangle(projected, (pixel[1] - box_width // 2, pixel[0] - box_height // 2), (pixel[1] + box_width // 2, pixel[0] + box_height // 2), (0, 0, 0), 2)
            # draw text
            org = (pixel[1] - 7 * (text_length), pixel[0] + 7)
            color = (255, 0, 0)
            cv2.putText(projected, str(keypoint_count), org, cv2.FONT_HERSHEY_SIMPLEX, 0.7, color, 2)
            keypoint_count += 1
        return projected

    @torch.inference_mode()
    @torch.amp.autocast('cuda')
    def _get_features(self, transformed_rgb, shape_info):
        img_h = shape_info['img_h']
        img_w = shape_info['img_w']
        patch_h = shape_info['patch_h']
        patch_w = shape_info['patch_w']
        # get features
        img_tensors = torch.from_numpy(transformed_rgb).permute(2, 0, 1).unsqueeze(0).to(self.device)  # float32 [1, 3, H, W]
        assert img_tensors.shape[1] == 3, "unexpected image shape"
        features_dict = self.dinov2.forward_features(img_tensors)
        raw_feature_grid = features_dict['x_norm_patchtokens']  # float32 [num_cams, patch_h*patch_w, feature_dim]
        raw_feature_grid = raw_feature_grid.reshape(1, patch_h, patch_w, -1)  # float32 [num_cams, patch_h, patch_w, feature_dim]
        # compute per-point feature using bilinear interpolation
        interpolated_feature_grid = interpolate(raw_feature_grid.permute(0, 3, 1, 2),  # float32 [num_cams, feature_dim, patch_h, patch_w]
                                                size=(img_h, img_w),
                                                mode='bilinear').permute(0, 2, 3, 1).squeeze(0)  # float32 [H, W, feature_dim]
        features_flat = interpolated_feature_grid.reshape(-1, interpolated_feature_grid.shape[-1])  # float32 [H*W, feature_dim]
        return features_flat

    def _cluster_features(self, points, features_flat, masks):
        candidate_keypoints = []
        candidate_pixels = []
        candidate_rigid_group_ids = []
        for rigid_group_id, binary_mask in enumerate(masks):
            # ignore mask that is too large
            if np.mean(binary_mask) > self.config['max_mask_ratio']:
                continue
            # consider only foreground features
            obj_features_flat = features_flat[binary_mask.reshape(-1)]
            feature_pixels = np.argwhere(binary_mask)
            if obj_features_flat.shape[0] == 0 or feature_pixels.shape[0] == 0:
                continue
            # reduce dimensionality to be less sensitive to noise and texture
            obj_features_flat = obj_features_flat.double()
            pca_dim = int(min(3, obj_features_flat.shape[1], max(1, obj_features_flat.shape[0])))
            if obj_features_flat.shape[0] >= 2 and pca_dim >= 1:
                (_, _, v) = torch.pca_lowrank(obj_features_flat, q=pca_dim, center=False)
                features_pca = torch.mm(obj_features_flat, v[:, :pca_dim])
            else:
                features_pca = obj_features_flat[:, :pca_dim]
            feat_min = features_pca.min(0)[0]
            feat_range = features_pca.max(0)[0] - feat_min
            feat_range = torch.where(feat_range > 1e-6, feat_range, torch.ones_like(feat_range))
            X = (features_pca - feat_min) / feat_range
            num_clusters = int(min(self.config['num_candidates_per_mask'], X.shape[0]))
            if num_clusters <= 0:
                continue
            # Cluster on DINO features only. Use the image-space centroid of each
            # cluster as the concrete 2D keypoint to match the original ReKep flow.
            if num_clusters == 1:
                cluster_ids_x = torch.zeros((X.shape[0],), dtype=torch.long, device=X.device)
            else:
                cluster_ids_x, _ = kmeans(
                    X=X,
                    num_clusters=num_clusters,
                    distance='euclidean',
                    device=self.device,
                )
            for cluster_id in range(num_clusters):
                member_idx = cluster_ids_x == cluster_id
                member_pixels = feature_pixels[member_idx]
                if member_pixels.shape[0] == 0:
                    continue
                centroid_vu = np.mean(member_pixels.astype(np.float64), axis=0)
                d2 = np.sum((member_pixels.astype(np.float64) - centroid_vu[None, :]) ** 2, axis=1)
                closest_idx = int(np.argmin(d2))
                candidate_pixel = member_pixels[closest_idx]
                point = points[int(candidate_pixel[0]), int(candidate_pixel[1])]
                if not np.all(np.isfinite(point)):
                    continue
                candidate_keypoints.append(point)
                candidate_pixels.append(candidate_pixel)
                candidate_rigid_group_ids.append(rigid_group_id)

        if candidate_keypoints:
            candidate_keypoints = np.asarray(candidate_keypoints, dtype=np.float64)
            candidate_pixels = np.asarray(candidate_pixels, dtype=np.int32)
            candidate_rigid_group_ids = np.asarray(candidate_rigid_group_ids, dtype=np.int32)
        else:
            candidate_keypoints = np.zeros((0, 3), dtype=np.float64)
            candidate_pixels = np.zeros((0, 2), dtype=np.int32)
            candidate_rigid_group_ids = np.zeros((0,), dtype=np.int32)

        return candidate_keypoints, candidate_pixels, candidate_rigid_group_ids

    def _merge_clusters(self, candidate_keypoints):
        if candidate_keypoints.shape[0] == 0:
            return []
        self.mean_shift.fit(candidate_keypoints)
        cluster_centers = self.mean_shift.cluster_centers_
        merged_indices = []
        for center in cluster_centers:
            dist = np.linalg.norm(candidate_keypoints - center, axis=-1)
            merged_indices.append(np.argmin(dist))
        return merged_indices
